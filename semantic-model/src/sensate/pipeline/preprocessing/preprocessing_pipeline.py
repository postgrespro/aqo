import re
import os
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Tuple, Optional
from tqdm import tqdm
from sensate.pipeline.preprocessing.sql_decomposer import decompose_operators_fast

# ---------------------------------------------------------------------------
# Module-level worker helpers  (must be top-level so they are picklable)
# ---------------------------------------------------------------------------
_pipeline_instance = None  # one instance per worker process


def _worker_init():
    """Initialise one PreprocessingPipeline per worker process."""
    global _pipeline_instance
    _pipeline_instance = PreprocessingPipeline()


def _worker_process_sql(sql: str) -> List[List[str]]:
    """Called inside each worker process; reuses the pre-created pipeline."""
    global _pipeline_instance
    return _pipeline_instance._sql_to_sentences(str(sql))


class PreprocessingPipeline:
    """
    SQL preprocessing pipeline that converts pure SQL to processed SQL with special tokens.
    
    Supports various RDBMS dialects and converts:
    - Table names -> <TAB>
    - Column names -> <COL>
    - Table aliases -> <ALIAS_T1>, <ALIAS_T2>, etc.
    - Output aliases -> <COL_OUT>
    - Literals -> <NUM>, <STR>, <DATE>, <TIMESTAMP>, <BOOL_TRUE>, <BOOL_FALSE>, <NULL>
    
    Example:
    "SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON o.user_id = u.id"
    -> "SELECT <ALIAS_T1>.<COL>, SUM(<ALIAS_T2>.<COL>) AS <COL_OUT> FROM <TAB> <ALIAS_T1> JOIN <TAB> <ALIAS_T2> ON <ALIAS_T2>.<COL> = <ALIAS_T1>.<COL>"
    """
    
    # SQL keywords that should remain uppercase
    KEYWORDS = {
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'ON', 'AND', 'OR', 'GROUP', 'BY', 'ORDER',
        'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'CROSS', 'UNION', 'DISTINCT',
        'LIMIT', 'OFFSET', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'ASC', 'DESC',
        'HAVING', 'IN', 'IS', 'NOT', 'NULL', 'INSERT', 'UPDATE', 'DELETE', 'VALUES',
        'SET', 'INTO', 'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'TRUE', 'FALSE', 'BETWEEN',
        'LIKE', 'EXISTS', 'ALL', 'ANY', 'TOP', 'WITH', 'USING', 'CREATE', 'TABLE',
        'DROP', 'ALTER', 'INDEX', 'VIEW', 'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES',
        'CONSTRAINT', 'UNIQUE', 'CHECK', 'DEFAULT', 'AUTO_INCREMENT', 'CASCADE',
        'DBO'  # Common schema prefix
    }
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset state for processing a new query."""
        self.table_aliases: Dict[str, int] = {}  # Maps alias name -> table number
        self.alias_counter = 0
        self.table_names: List[str] = []  # Track table names in order
        self.output_aliases: List[str] = []  # Track output column aliases from SELECT ... AS
    
    def tokenize(self, sql: str) -> str:
        """
        Main method that converts pure SQL to processed SQL string.
        Returns a processed SQL string with special tokens.
        """
        if not sql or not sql.strip():
            return ""
        
        self.reset()
        query = sql.strip()

        # Step 0: Normalise PostgreSQL ARRAY[...] → (...) so numeric/string
        # elements inside are handled by the regular tokenizer instead of
        # being swallowed whole by the bracket-identifier pattern.
        query = re.sub(r'\bARRAY\s*\[', '(', query, flags=re.IGNORECASE)

        # Step 1: Extract table aliases from the query FIRST (before any modifications)
        self._extract_table_aliases(query)
        
        # Step 2: Extract output aliases from SELECT ... AS ...
        self._extract_output_aliases(query)
        
        # Step 3: Tokenize the query - split into words, operators, etc.
        tokens = self._tokenize_query(query)
        
        # Step 4: Process tokens and classify them
        processed_tokens = self._process_tokens(tokens)
        
        # Step 5: Join back into a string
        return ' '.join(processed_tokens)
    
    def _extract_table_aliases(self, query: str):
        """
        Extract all table aliases from the query and assign them numbers.
        Handles: FROM table alias, JOIN table alias, etc.
        """
        # Match FROM clause with aliases: FROM table_name alias or FROM table_name AS alias
        from_pattern = r'\bFROM\s+([#\w]+)\s+(?:AS\s+)?([a-zA-Z_]\w*)\b'
        for match in re.finditer(from_pattern, query, re.IGNORECASE):
            table_name = match.group(1)
            alias = match.group(2)
            if alias.upper() not in self.KEYWORDS:
                self._register_alias(alias)
                self.table_names.append(table_name)
        
        # Match JOIN clauses with aliases: JOIN table_name alias or JOIN table_name AS alias
        join_pattern = r'\bJOIN\s+([#\w]+)\s+(?:AS\s+)?([a-zA-Z_]\w*)\b'
        for match in re.finditer(join_pattern, query, re.IGNORECASE):
            table_name = match.group(1)
            alias = match.group(2)
            if alias.upper() not in self.KEYWORDS:
                self._register_alias(alias)
                self.table_names.append(table_name)
    
    def _register_alias(self, alias: str):
        """Register an alias and assign it a number if not already registered."""
        alias_lower = alias.lower()
        if alias_lower not in self.table_aliases:
            self.alias_counter += 1
            self.table_aliases[alias_lower] = self.alias_counter
    
    def _extract_output_aliases(self, query: str):
        """
        Extract output aliases from SELECT clause (AS alias_name).
        These appear after AS in the SELECT portion of the query.
        """
        # Match AS alias_name pattern, but only in SELECT clause (before FROM)
        # Simple approach: find everything between SELECT and FROM
        select_match = re.search(r'\bSELECT\b(.*?)\bFROM\b', query, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_clause = select_match.group(1)
            # Find all AS alias patterns, excluding bracketed ones for now
            # Pattern: AS followed by an identifier (not a keyword)
            as_pattern = r'\bAS\s+([a-zA-Z_]\w*)\b'
            for match in re.finditer(as_pattern, select_clause, re.IGNORECASE):
                alias = match.group(1)
                if alias.upper() not in self.KEYWORDS:
                    self.output_aliases.append(alias.lower())
            
            # Also handle bracketed aliases [name]
            bracket_pattern = r'\bAS\s+\[([^\]]+)\]'
            for match in re.finditer(bracket_pattern, select_clause, re.IGNORECASE):
                alias = match.group(1)
                self.output_aliases.append(alias.lower())
    
    def _tokenize_query(self, query: str) -> List[str]:
        """
        Tokenize SQL query into a list of tokens.
        Preserves strings, numbers, operators, identifiers, etc.
        """
        # Regex pattern to match:
        # - Strings in quotes: '...' or "..."
        # - Numbers: integers and decimals
        # - Operators: =, !=, <=, >=, <>, etc.
        # - Identifiers: table names, column names, keywords
        # - Punctuation: (, ), ,, ;, .
        # - Bracketed identifiers: [name]
        pattern = r"""
            '(?:[^']|'')*'|           # Single-quoted strings
            "(?:[^"]|"")*"|           # Double-quoted strings
            \d+\.?\d*|                # Numbers (int and decimal)
            <>|!=|<=|>=|              # Multi-char operators
            [a-zA-Z_#][\w]*|          # Identifiers (including # for temp tables)
            \[[^\]'"]+\]|              # Bracketed identifiers [name] (no quotes inside)
            [(),;.=<>+\-*/]           # Single-char operators and punctuation
        """
        tokens = re.findall(pattern, query, re.VERBOSE | re.IGNORECASE)
        return tokens
    
    def _process_tokens(self, tokens: List[str]) -> List[str]:
        """
        Process tokens and classify them as keywords, tables, columns, literals, etc.

        Only the following tokens are masked (replaced with special tokens):
          - <ALIAS_T(i)>  : table alias
          - <COL_OUT>     : output alias from SELECT ... AS ...
          - <NUM>         : numeric literal
          - <STR>         : string literal
          - <DATE>        : date literal ('YYYY-MM-DD')
          - <TIMESTAMP>   : timestamp literal ('YYYY-MM-DD HH:MM:SS' / ISO 8601)
          - <BOOL_TRUE>   : boolean TRUE literal
          - <BOOL_FALSE>  : boolean FALSE literal
          - <NULL>        : NULL literal

        Everything else (table names, column names, keywords, operators, etc.)
        is kept as-is.
        """
        result = []
        i = 0
        in_select_clause = False
        after_as_in_select = False
        after_as_in_from = False
        expect_table = False
        expect_table_alias = False
        
        while i < len(tokens):
            token = tokens[i]
            token_upper = token.upper()
            
            # Skip whitespace
            if not token or token.isspace():
                i += 1
                continue
            
            # Handle string literals -> <DATE>, <TIMESTAMP>, or <STR>
            if self._is_string_literal(token):
                inner = token[1:-1]  # strip surrounding quotes
                if self._is_timestamp(inner):
                    result.append('<TIMESTAMP>')
                elif self._is_date(inner):
                    result.append('<DATE>')
                else:
                    result.append('<STR>')
                i += 1
                continue
            
            # Handle numeric literals -> <NUM>
            if self._is_number(token):
                result.append('<NUM>')
                i += 1
                continue
            
            # Handle boolean / null literals before keyword check
            if token_upper == 'TRUE':
                result.append('<BOOL_TRUE>')
                i += 1
                continue

            if token_upper == 'FALSE':
                result.append('<BOOL_FALSE>')
                i += 1
                continue

            if token_upper == 'NULL':
                result.append('<NULL>')
                i += 1
                continue

            # Handle keywords
            if token_upper == 'SELECT':
                result.append('SELECT')
                in_select_clause = True
                i += 1
                continue
            
            if token_upper == 'FROM':
                result.append('FROM')
                in_select_clause = False
                expect_table = True
                i += 1
                continue
            
            if token_upper == 'JOIN' or token_upper in ('INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'CROSS'):
                result.append(token_upper)
                if token_upper == 'JOIN':
                    expect_table = True
                i += 1
                continue
            
            if token_upper == 'AS':
                result.append('AS')
                if in_select_clause:
                    after_as_in_select = True
                else:
                    after_as_in_from = True
                i += 1
                continue
            
            if token_upper in self.KEYWORDS:
                result.append(token_upper)
                after_as_in_select = False
                after_as_in_from = False
                if token_upper in ('WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT'):
                    in_select_clause = False
                i += 1
                continue
            
            # Handle qualified column references: alias.column  ->  <ALIAS_Ti>.column
            if i + 2 < len(tokens) and tokens[i + 1] == '.':
                alias_or_schema = token.lower()
                column = tokens[i + 2]
                
                # Check if it's a known table alias
                if alias_or_schema in self.table_aliases:
                    alias_num = self.table_aliases[alias_or_schema]
                    result.append(f'<ALIAS_T{alias_num}>')
                    result.append('.')
                    result.append(column)  # keep actual column name
                    i += 3
                    after_as_in_select = False
                    after_as_in_from = False
                    continue
                else:
                    # schema.table or schema.function - keep actual names
                    result.append(token)
                    result.append('.')
                    result.append(column)
                    i += 3
                    after_as_in_select = False
                    after_as_in_from = False
                    continue
            
            # Handle table names (after FROM or JOIN) - keep actual name
            if expect_table:
                result.append(token)  # keep actual table name
                expect_table = False
                expect_table_alias = True
                i += 1
                continue
            
            # Handle table aliases (after table name in FROM/JOIN clause)
            if expect_table_alias:
                # This could be a table alias or the next keyword
                if token_upper in self.KEYWORDS and token_upper != 'AS':
                    # Not an alias, it's a keyword - no alias provided
                    result.append(token_upper)
                    expect_table_alias = False
                    if token_upper == 'JOIN':
                        expect_table = True
                    elif token_upper in ('WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT'):
                        in_select_clause = False
                elif token.lower() in self.table_aliases:
                    # This is an alias
                    alias_num = self.table_aliases[token.lower()]
                    result.append(f'<ALIAS_T{alias_num}>')
                    expect_table_alias = False
                    after_as_in_from = False
                    i += 1
                    continue
                else:
                    # Unknown identifier after table - keep as-is
                    result.append(token)
                    expect_table_alias = False
                i += 1
                continue
            
            # Handle output aliases (after AS in SELECT clause)
            if after_as_in_select:
                if token.startswith('[') and token.endswith(']'):
                    result.append('<COL_OUT>')
                elif token_upper not in self.KEYWORDS:
                    result.append('<COL_OUT>')
                else:
                    # It's a keyword, not an alias
                    result.append(token_upper)
                after_as_in_select = False
                i += 1
                continue
            
            # Handle table aliases after AS in FROM/JOIN clause
            if after_as_in_from:
                if token.lower() in self.table_aliases:
                    alias_num = self.table_aliases[token.lower()]
                    result.append(f'<ALIAS_T{alias_num}>')
                else:
                    result.append(token)  # keep as-is
                after_as_in_from = False
                i += 1
                continue
            
            # Handle bracketed identifiers - keep as-is (or <COL_OUT> when after AS)
            if token.startswith('[') and token.endswith(']'):
                if after_as_in_select:
                    result.append('<COL_OUT>')
                    after_as_in_select = False
                else:
                    inner_name = token[1:-1]  # strip [ and ]
                    result.append(inner_name)
                i += 1
                continue
            
            # Handle function calls - keep function name as-is
            if i + 1 < len(tokens) and tokens[i + 1] == '(':
                result.append(token_upper)
                i += 1
                continue
            
            # Handle punctuation
            if token in '(),;.=<>!+-*/':
                result.append(token)
                i += 1
                continue
            
            # Handle standalone aliases (when they appear without dot notation)
            if token.lower() in self.table_aliases:
                alias_num = self.table_aliases[token.lower()]
                result.append(f'<ALIAS_T{alias_num}>')
                i += 1
                continue
            
            # Check if it's an output alias (from SELECT ... AS ...)
            # These can appear in ORDER BY, GROUP BY, HAVING clauses
            if token.lower() in self.output_aliases:
                result.append('<COL_OUT>')
                i += 1
                continue
            
            # Everything else (column names, identifiers) - keep as-is
            result.append(token)
            i += 1
        
        return result
    
    def _is_string_literal(self, token: str) -> bool:
        """Check if token is a string literal."""
        return (token.startswith("'") and token.endswith("'")) or \
               (token.startswith('"') and token.endswith('"'))
    
    def _is_date(self, s: str) -> bool:
        """Check if string matches a date pattern: YYYY-MM-DD."""
        return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', s.strip()))

    def _is_timestamp(self, s: str) -> bool:
        """Check if string matches a timestamp pattern: YYYY-MM-DD HH:MM[:SS[.fff]]."""
        return bool(re.match(
            r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?',
            s.strip()
        ))

    def _is_number(self, token: str) -> bool:
        """Check if token is a number."""
        try:
            float(token)
            return True
        except ValueError:
            return False
    
    def _sql_to_sentences(self, sql: str) -> List[List[str]]:
        """
        Decompose one SQL into sub-operator fragments (plan-tree nodes),
        each tokenized as a separate training sentence.
        If no fragments are found, the SQL is discarded entirely.
        """
        fragments = decompose_operators_fast(sql)
        if not fragments:
            return []

        sentences = []
        for frag in fragments:
            tokens = self.tokenize(frag).split()
            if tokens:
                sentences.append(tokens)
        return sentences

    def __call__(self, query_or_batch, num_workers: int = 1):
        """
        Process a single query or a batch of queries.
        Each SQL is decomposed into sub-operator fragments via sql_decomposer
        so that Word2Vec learns the semantics of individual plan-tree nodes.

        Args:
            query_or_batch: A single SQL string or an iterable of SQL strings.
            num_workers: Number of parallel worker processes (default 1 = sequential).
                         Set to os.cpu_count() or the config value for fastest throughput.

        Returns:
            For a single string: list of token lists (one per fragment).
            For a batch: flat list of token lists across all queries.
        """
        if isinstance(query_or_batch, str):
            return self._sql_to_sentences(query_or_batch)

        batch = list(query_or_batch)
        total_queries = len(batch)

        if num_workers > 1:
            # ------------------------------------------------------------------
            # Parallel path — one PreprocessingPipeline instance per worker,
            # created via the initialiser (not recreated per task).
            # chunksize=500 amortises IPC overhead for small tasks.
            # ------------------------------------------------------------------
            chunksize = max(1, min(500, total_queries // (num_workers * 4)))
            all_results: List[List[List[str]]] = []
            with ProcessPoolExecutor(
                max_workers=num_workers,
                initializer=_worker_init,
            ) as executor:
                for result in tqdm(
                    executor.map(_worker_process_sql, batch, chunksize=chunksize),
                    total=total_queries,
                    desc=f"Preprocessing SQL (workers={num_workers})",
                ):
                    all_results.append(result)
        else:
            # ------------------------------------------------------------------
            # Sequential fallback (num_workers == 1)
            # ------------------------------------------------------------------
            all_results = [
                self._sql_to_sentences(str(q))
                for q in tqdm(batch, desc="Decomposing & preprocessing SQL")
            ]

        sentences = []
        discarded = 0
        for sents in all_results:
            if not sents:
                discarded += 1
            else:
                sentences.extend(sents)

        kept = total_queries - discarded
        avg_frags = len(sentences) / kept if kept > 0 else 0
        avg_len = sum(len(s) for s in sentences) / len(sentences) if sentences else 0
        print(f"\n📋 Preprocessing summary:")
        print(f"   Total queries       : {total_queries:,}")
        print(f"   Discarded (no frags): {discarded:,}")
        print(f"   Kept queries        : {kept:,}")
        print(f"   Total sub-sentences : {len(sentences):,}  (avg {avg_frags:.1f} frags/query)")
        print(f"   Avg tokens/sentence : {avg_len:.1f}")
        return sentences
