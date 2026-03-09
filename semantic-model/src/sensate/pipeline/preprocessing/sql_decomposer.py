import re
import hashlib
from concurrent.futures import ProcessPoolExecutor
import os


JOIN_PATTERN = re.compile(
    r"JOIN\s+\w+(?:\s+\w+)?\s+ON\s+(.*?)(?=JOIN|WHERE|$)",
    re.DOTALL,
)

WHERE_PATTERN = re.compile(
    r"WHERE\s+(.*?)(?=GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|$)",
    re.DOTALL,
)

AND_SPLIT = re.compile(r"\s+AND\s+")

TABLE_ALIAS_PATTERN = re.compile(
    r"(FROM|JOIN)\s+(\w+)(?:\s+AS)?\s+(\w+)?",
    re.IGNORECASE,
)
def safe_and_split(condition_str):
    parts = []
    current = []
    depth = 0
    i = 0
    length = len(condition_str)

    while i < length:
        if condition_str[i] == "(":
            depth += 1
        elif condition_str[i] == ")":
            depth -= 1

        if depth == 0 and condition_str[i:i+3] == "AND":
            parts.append("".join(current).strip())
            current = []
            i += 3
            continue

        current.append(condition_str[i])
        i += 1

    if current:
        parts.append("".join(current).strip())

    return parts
def contains_subquery(cond: str):
    return re.search(r"\(\s*SELECT", cond, re.IGNORECASE)

FROM_CLAUSE_PATTERN = re.compile(
    r"\bFROM\b\s+(.*?)(?=\bWHERE\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|\bHAVING\b|\bLIMIT\b|$)",
    re.IGNORECASE | re.DOTALL,
)

def safe_comma_split(s: str):
    parts, cur, depth = [], [], 0
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            part = "".join(cur).strip()
            if part:
                parts.append(part)
            cur = []
        else:
            cur.append(ch)
    last = "".join(cur).strip()
    if last:
        parts.append(last)
    return parts

def parse_table_alias(fragment: str):
    fragment = fragment.strip()
    fragment = re.sub(r"\s+", " ", fragment)
    m = re.match(r"^(\w+(?:\.\w+)?)(?:\s+(?:AS\s+)?(\w+))?$", fragment, re.IGNORECASE)
    if not m:
        return None, None
    table = m.group(1)
    alias = m.group(2)
    return table, alias

def extract_alias_mapping(sql: str):
    mapping = {}

    m = FROM_CLAUSE_PATTERN.search(sql)
    if m:
        from_body = m.group(1)
        for frag in safe_comma_split(from_body):
            table, alias = parse_table_alias(frag)
            if not table:
                continue
            table_u = table.upper().split(".")[-1]  # nếu có schema thì lấy tên bảng
            mapping[table_u] = table_u
            if alias:
                mapping[alias.upper()] = table_u

    for _, table, alias in TABLE_ALIAS_PATTERN.findall(sql):
        table_u = table.upper()
        mapping[table_u] = table_u
        if alias:
            mapping[alias.upper()] = table_u

    return mapping

def replace_alias_with_table(condition: str, alias_map: dict):
    for alias, table in alias_map.items():
        condition = re.sub(rf"\b{alias}\.", f"{table}.", condition)
    return condition

def is_join_condition(cond: str):
    m = re.match(r"(\w+\.\w+)\s*=\s*(\w+\.\w+)", cond)
    if not m:
        return False

    left_table = m.group(1).split(".")[0]
    right_table = m.group(2).split(".")[0]

    return left_table != right_table


def decompose_operators_fast(sql: str):

    sql = sql.upper()

    alias_map = extract_alias_mapping(sql)

    join_ops = []
    filter_ops = []

    explicit_join = JOIN_PATTERN.findall(sql)
    explicit_join = [j.strip() for j in reversed(explicit_join)]
    explicit_join = [
        replace_alias_with_table(op, alias_map)
        for op in explicit_join
    ]

    join_ops.extend(explicit_join)
    m = WHERE_PATTERN.search(sql)
    if m:
        conditions = m.group(1)
        parts = safe_and_split(conditions)

        for cond in parts:
            cond = cond.strip()
            cond = replace_alias_with_table(cond, alias_map)

            if contains_subquery(cond):
                subqueries = re.findall(r"\((\s*SELECT.*?\))", cond, re.DOTALL)
                for sq in subqueries:
                    join_ops.extend(decompose_operators_fast(sq))
                filter_ops.append(cond)
                continue

            if is_join_condition(cond):
                join_ops.append(cond)
            else:
                filter_ops.append(cond)

    return join_ops + filter_ops


def operator_hash(op: str):
    return hashlib.md5(op.encode()).hexdigest()



def process_single_sql(sql: str):
    ops = decompose_operators_fast(sql)
    hashed = [operator_hash(op) for op in ops]

    return {
        "operators": ops,      # giữ nguyên số
        "hashes": hashed
    }



def process_sql_batch_parallel(sql_list, workers=None):
    if workers is None:
        workers = os.cpu_count()

    with ProcessPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(process_single_sql, sql_list))

    return results


if __name__ == "__main__":

    query_original = """
    SELECT u.name, SUM(o.amount) AS total
    FROM users u
    JOIN orders o ON o.user_id = u.id
    WHERE u.age >= 21 AND o.status IN ('paid','shipped')
    GROUP BY u.name
    ORDER BY total DESC
    """

    query_extended = """
        SELECT MAX(mc.note) AS production_note, MAX(t.title) AS movie_title, MAX(t.production_year) AS movie_year FROM company_type AS ct, info_type AS it, movie_companies AS mc, movie_info_idx AS mi_idx, title AS t WHERE t.id = mc.movie_id AND t.id = mi_idx.movie_id AND ct.id = mc.company_type_id AND it.info = 'top 250 rank' AND (mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%') AND ct.kind = 'production companies' AND it.id = mi_idx.info_type_id AND mc.movie_id = mi_idx.movie_id AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'

    """

    batch = [query_original, query_extended]

    results = process_sql_batch_parallel(batch)

    for idx, r in enumerate(results):
        print(f"\n===== Query {idx} =====")
        for i, op in enumerate(r["operators"], 1):
            print(f"Node {i}: {op}")