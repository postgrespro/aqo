#include "sql_preprocessor.h"
#include <ctype.h>
#include "pg_compat.h"

#define INITIAL_ARRAY_CAPACITY 32

/* SQL keywords to preserve */
static const char *SQL_KEYWORDS[] = {
    "SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER",
    "ON", "AND", "OR", "NOT", "IN", "IS", "NULL", "AS", "GROUP", "BY", "ORDER",
    "HAVING", "LIMIT", "OFFSET", "UNION", "DISTINCT", "CASE", "WHEN", "THEN",
    "ELSE", "END", "ASC", "DESC", "LIKE", "BETWEEN", "EXISTS", "ALL", "ANY",
    "INSERT", "UPDATE", "DELETE", "VALUES", "SET", "INTO", "CREATE", "DROP",
    "ALTER", "TABLE", "INDEX", "VIEW", "PRIMARY", "KEY", "FOREIGN", "REFERENCES",
    "SUM", "COUNT", "AVG", "MIN", "MAX", "CAST", "COALESCE", "NULLIF",
    NULL
};

/* Context state for processing */
typedef struct {
    bool in_select_clause;
    bool expect_table_name;
    bool expect_alias_after_table;
    bool after_as_keyword;
    bool expect_output_alias;
    AliasMap *aliases;
    OutputAliasMap *output_aliases;
    int alias_counter;
} ProcessingContext;

/* Token array functions */
TokenArray* create_token_array(void) {
    TokenArray *arr = (TokenArray*)palloc(sizeof(TokenArray));
    if (!arr) return NULL;

    arr->tokens = (char**)palloc(INITIAL_ARRAY_CAPACITY * sizeof(char*));
    if (!arr->tokens) {
        pfree(arr);
        return NULL;
    }

    arr->count = 0;
    arr->capacity = INITIAL_ARRAY_CAPACITY;
    return arr;
}

bool add_token_to_array(TokenArray *arr, const char *token) {
    if (!arr || !token) return false;

    if (arr->count >= arr->capacity) {
        size_t new_capacity = arr->capacity * 2;
        char **new_tokens = (char**)repalloc(arr->tokens, new_capacity * sizeof(char*));
        if (!new_tokens) return false;
        arr->tokens = new_tokens;
        arr->capacity = new_capacity;
    }

    arr->tokens[arr->count] = pstrdup(token);
    if (!arr->tokens[arr->count]) return false;

    arr->count++;
    return true;
}

void free_token_array(TokenArray *arr) {
    if (!arr) return;

    for (size_t i = 0; i < arr->count; i++) {
        pfree(arr->tokens[i]);
    }
    pfree(arr->tokens);
    pfree(arr);
}

void free_sql_preprocessing_result(SQLPreprocessingResult *result) {
    if (!result) return;

    if (result->processed_query) {
        pfree(result->processed_query);
    }
    if (result->tokens) {
        free_token_array(result->tokens);
    }
    pfree(result);
}

/* Alias map functions */
static void init_alias_map(AliasMap *map) {
    map->count = 0;
    for (int i = 0; i < MAX_ALIASES; i++) {
        map->entries[i].alias_name = NULL;
        map->entries[i].alias_number = 0;
    }
}

static void free_alias_map(AliasMap *map) {
    if (!map) return;
    for (int i = 0; i < map->count; i++) {
        if (map->entries[i].alias_name) {
            pfree(map->entries[i].alias_name);
        }
    }
}

/* Output alias map functions */
static void init_output_alias_map(OutputAliasMap *map) {
    map->count = 0;
    for (int i = 0; i < MAX_OUTPUT_ALIASES; i++) {
        map->names[i] = NULL;
    }
}

static void free_output_alias_map(OutputAliasMap *map) {
    if (!map) return;
    for (int i = 0; i < map->count; i++) {
        if (map->names[i]) {
            pfree(map->names[i]);
        }
    }
}

static bool add_output_alias(OutputAliasMap *map, const char *alias) {
    if (!map || !alias || map->count >= MAX_OUTPUT_ALIASES) return false;

    char lower[MAX_TOKEN_LENGTH];
    size_t len = strlen(alias);
    for (size_t i = 0; i < len && i < MAX_TOKEN_LENGTH - 1; i++) {
        lower[i] = tolower(alias[i]);
    }
    lower[len < MAX_TOKEN_LENGTH ? len : MAX_TOKEN_LENGTH - 1] = '\0';

    map->names[map->count] = pstrdup(lower);
    if (!map->names[map->count]) return false;

    map->count++;
    return true;
}

static bool is_output_alias(const OutputAliasMap *map, const char *token) {
    if (!map || !token) return false;

    char lower[MAX_TOKEN_LENGTH];
    size_t len = strlen(token);
    for (size_t i = 0; i < len && i < MAX_TOKEN_LENGTH - 1; i++) {
        lower[i] = tolower(token[i]);
    }
    lower[len < MAX_TOKEN_LENGTH ? len : MAX_TOKEN_LENGTH - 1] = '\0';

    for (int i = 0; i < map->count; i++) {
        if (strcmp(map->names[i], lower) == 0) {
            return true;
        }
    }
    return false;
}

static int find_alias(const AliasMap *map, const char *alias) {
    if (!map || !alias) return -1;

    char lower[MAX_TOKEN_LENGTH];
    size_t len = strlen(alias);
    for (size_t i = 0; i < len && i < MAX_TOKEN_LENGTH - 1; i++) {
        lower[i] = tolower(alias[i]);
    }
    lower[len < MAX_TOKEN_LENGTH ? len : MAX_TOKEN_LENGTH - 1] = '\0';

    for (int i = 0; i < map->count; i++) {
        if (strcmp(map->entries[i].alias_name, lower) == 0) {
            return map->entries[i].alias_number;
        }
    }
    return -1;
}

static bool register_alias(AliasMap *map, const char *alias, int *counter) {
    if (!map || !alias || map->count >= MAX_ALIASES) return false;

    /* Check if already registered */
    if (find_alias(map, alias) != -1) return true;

    char lower[MAX_TOKEN_LENGTH];
    size_t len = strlen(alias);
    for (size_t i = 0; i < len && i < MAX_TOKEN_LENGTH - 1; i++) {
        lower[i] = tolower(alias[i]);
    }
    lower[len < MAX_TOKEN_LENGTH ? len : MAX_TOKEN_LENGTH - 1] = '\0';

    map->entries[map->count].alias_name = pstrdup(lower);
    if (!map->entries[map->count].alias_name) return false;

    (*counter)++;
    map->entries[map->count].alias_number = *counter;
    map->count++;

    return true;
}

/* Check if string is a SQL keyword */
static bool is_keyword(const char *str) {
    if (!str) return false;

    char upper[MAX_TOKEN_LENGTH];
    size_t len = strlen(str);
    if (len >= MAX_TOKEN_LENGTH) return false;

    for (size_t i = 0; i < len; i++) {
        upper[i] = toupper(str[i]);
    }
    upper[len] = '\0';

    for (int i = 0; SQL_KEYWORDS[i] != NULL; i++) {
        if (strcmp(upper, SQL_KEYWORDS[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Check if character is valid in identifier */
static bool is_identifier_char(char c) {
    return isalnum(c) || c == '_' || c == '#';
}

/* Check if string is a number */
static bool is_number(const char *str) {
    if (!str || !*str) return false;

    const char *p = str;
    if (*p == '-' || *p == '+') p++;

    bool has_digit = false;
    bool has_dot = false;

    while (*p) {
        if (isdigit(*p)) {
            has_digit = true;
        } else if (*p == '.' && !has_dot) {
            has_dot = true;
        } else {
            return false;
        }
        p++;
    }

    return has_digit;
}

/* Skip whitespace */
static const char* skip_whitespace(const char *str) {
    while (*str && isspace(*str)) str++;
    return str;
}

/* Extract next token from SQL query */
static const char* extract_token(const char *sql, char *token_buf, size_t buf_size) {
    const char *p = skip_whitespace(sql);
    if (!*p) return NULL;

    size_t idx = 0;

    /* String literal (single quote) */
    if (*p == '\'') {
        token_buf[idx++] = *p++;
        while (*p && idx < buf_size - 1) {
            token_buf[idx++] = *p;
            if (*p == '\'') {
                p++;
                if (*p == '\'') {
                    token_buf[idx++] = *p++;
                } else {
                    break;
                }
            } else {
                p++;
            }
        }
        token_buf[idx] = '\0';
        return p;
    }

    /* Identifier or keyword */
    if (isalpha(*p) || *p == '_' || *p == '#') {
        while (*p && is_identifier_char(*p) && idx < buf_size - 1) {
            token_buf[idx++] = *p++;
        }
        token_buf[idx] = '\0';
        return p;
    }

    /* Number */
    if (isdigit(*p) || (*p == '-' && isdigit(*(p + 1)))) {
        if (*p == '-') {
            token_buf[idx++] = *p++;
        }
        while (*p && (isdigit(*p) || *p == '.') && idx < buf_size - 1) {
            token_buf[idx++] = *p++;
        }
        token_buf[idx] = '\0';
        return p;
    }

    /* Multi-character operators */
    if (*p == '<' && *(p + 1) == '=') {
        token_buf[0] = '<'; token_buf[1] = '='; token_buf[2] = '\0';
        return p + 2;
    }
    if (*p == '>' && *(p + 1) == '=') {
        token_buf[0] = '>'; token_buf[1] = '='; token_buf[2] = '\0';
        return p + 2;
    }
    if (*p == '<' && *(p + 1) == '>') {
        token_buf[0] = '<'; token_buf[1] = '>'; token_buf[2] = '\0';
        return p + 2;
    }
    if (*p == '!' && *(p + 1) == '=') {
        token_buf[0] = '!'; token_buf[1] = '='; token_buf[2] = '\0';
        return p + 2;
    }

    /* Single character operators/punctuation */
    if (strchr("(),;.=<>+-*/", *p)) {
        token_buf[0] = *p;
        token_buf[1] = '\0';
        return p + 1;
    }

    /* Unknown character */
    token_buf[0] = *p;
    token_buf[1] = '\0';
    return p + 1;
}

/* Extract and register all aliases from query */
static void extract_all_aliases(const char *sql, AliasMap *map, int *counter) {
    char token_buf[MAX_TOKEN_LENGTH];
    const char *p = sql;
    bool after_from_or_join = false;
    bool seen_table_name = false;

    while (p && *p) {
        p = extract_token(p, token_buf, MAX_TOKEN_LENGTH);
        if (!p) break;

        if (is_keyword(token_buf)) {
            char upper[MAX_TOKEN_LENGTH];
            size_t len = strlen(token_buf);
            for (size_t i = 0; i < len; i++) {
                upper[i] = toupper(token_buf[i]);
            }
            upper[len] = '\0';

            if (strcmp(upper, "FROM") == 0 || strcmp(upper, "JOIN") == 0) {
                after_from_or_join = true;
                seen_table_name = false;
            } else if (strcmp(upper, "AS") == 0) {
                /* Skip AS keyword */
                continue;
            } else {
                after_from_or_join = false;
                seen_table_name = false;
            }
        } else if (after_from_or_join && !seen_table_name) {
            /* This is a table name */
            seen_table_name = true;
        } else if (after_from_or_join && seen_table_name) {
            /* This is an alias after table name */
            if (!is_keyword(token_buf) && !is_number(token_buf) && token_buf[0] != '\'' &&
                !strchr("(),;.=<>+-*/!", token_buf[0])) {
                register_alias(map, token_buf, counter);
            }
            after_from_or_join = false;
            seen_table_name = false;
        }
    }
}

/* Classify and process token with context */
static const char* classify_token_with_context(const char *token, ProcessingContext *ctx, const char *next_token) {
    static char buf[MAX_TOKEN_LENGTH];

    if (!token || !*token) return "";

    /* String literal */
    if (token[0] == '\'') {
        return "<STR>";
    }

    /* Number */
    if (is_number(token)) {
        return "<NUM>";
    }

    char upper[MAX_TOKEN_LENGTH];
    size_t len = strlen(token);
    for (size_t i = 0; i < len; i++)
        upper[i] = toupper(token[i]);
    upper[len] = '\0';

    if (strcmp(upper, "TRUE") == 0) return "<BOOL_TRUE>";
    if (strcmp(upper, "FALSE") == 0) return "<BOOL_FALSE>";
    if (strcmp(upper, "NULL") == 0) return "<NULL>";

    /* SQL keywords */
    if (is_keyword(token)) {

        strcpy(buf, upper);

        if (strcmp(buf, "SELECT") == 0) {
            ctx->in_select_clause = true;
        }

        else if (strcmp(buf, "FROM") == 0) {
            ctx->in_select_clause = false;
            ctx->expect_table_name = true;
        }

        else if (strcmp(buf, "JOIN") == 0) {
            ctx->expect_table_name = true;
        }

        else if (strcmp(buf, "AS") == 0) {

            if (ctx->in_select_clause)
                ctx->expect_output_alias = true;
            else
                ctx->after_as_keyword = true;
        }

        else if (
            strcmp(buf, "WHERE") == 0 ||
            strcmp(buf, "GROUP") == 0 ||
            strcmp(buf, "ORDER") == 0 ||
            strcmp(buf, "HAVING") == 0 ||
            strcmp(buf, "LIMIT") == 0
        ) {
            ctx->in_select_clause = false;
        }

        return buf;
    }

    /* punctuation */
    if (strchr("(),;.=<>+-*/", token[0]) && strlen(token) <= 2) {
        return token;
    }

    /* table name after FROM/JOIN */
    if (ctx->expect_table_name) {

        ctx->expect_table_name = false;
        ctx->expect_alias_after_table = true;

        /* Python logic: keep table name */
        return token;
    }

    /* alias after table */
    if (ctx->expect_alias_after_table) {

        ctx->expect_alias_after_table = false;

        int alias_num = find_alias(ctx->aliases, token);

        if (alias_num != -1) {
            snprintf(buf, MAX_TOKEN_LENGTH, "<ALIAS_T%d>", alias_num);
            return buf;
        }

        return token;
    }

    /* output alias after AS in SELECT */
    if (ctx->expect_output_alias) {

        ctx->expect_output_alias = false;

        add_output_alias(ctx->output_aliases, token);

        return "<COL_OUT>";
    }

    /* alias after AS in FROM/JOIN */
    if (ctx->after_as_keyword) {

        ctx->after_as_keyword = false;

        int alias_num = find_alias(ctx->aliases, token);

        if (alias_num != -1) {
            snprintf(buf, MAX_TOKEN_LENGTH, "<ALIAS_T%d>", alias_num);
            return buf;
        }

        return token;
    }

    /* registered alias standalone */
    int alias_num = find_alias(ctx->aliases, token);

    if (alias_num != -1) {
        snprintf(buf, MAX_TOKEN_LENGTH, "<ALIAS_T%d>", alias_num);
        return buf;
    }

    /* output alias used in ORDER BY / GROUP BY */
    if (is_output_alias(ctx->output_aliases, token)) {
        return "<COL_OUT>";
    }

    /* default: keep identifier (column/table/schema) */
    return token;
}

/* Main preprocessing function */
SQLPreprocessingResult* preprocess_sql_query(const char *sql) {
    if (!sql) return NULL;

    SQLPreprocessingResult *result = (SQLPreprocessingResult*)palloc(sizeof(SQLPreprocessingResult));
    if (!result) return NULL;

    result->processed_query = NULL;
    result->tokens = NULL;
    result->success = false;

    /* Create token array */
    result->tokens = create_token_array();
    if (!result->tokens) {
        free_sql_preprocessing_result(result);
        return NULL;
    }

    /* Initialize alias map and extract aliases */
    AliasMap alias_map;
    init_alias_map(&alias_map);
    int alias_counter = 0;
    extract_all_aliases(sql, &alias_map, &alias_counter);

    /* Initialize output alias map */
    OutputAliasMap output_alias_map;
    init_output_alias_map(&output_alias_map);

    /* Initialize processing context */
    ProcessingContext ctx = {
        .in_select_clause = false,
        .expect_table_name = false,
        .expect_alias_after_table = false,
        .after_as_keyword = false,
        .expect_output_alias = false,
        .aliases = &alias_map,
        .output_aliases = &output_alias_map,
        .alias_counter = alias_counter
    };

    /* Tokenize and classify */
    char token_buf[MAX_TOKEN_LENGTH];
    char next_token_buf[MAX_TOKEN_LENGTH];
    const char *p = sql;
    size_t total_length = 0;

    while (p && *p) {
        p = extract_token(p, token_buf, MAX_TOKEN_LENGTH);
        if (!p) break;

        /* Peek next token */
        const char *peek = extract_token(p, next_token_buf, MAX_TOKEN_LENGTH);
        const char *next_tok = peek ? next_token_buf : NULL;

        const char *classified = classify_token_with_context(token_buf, &ctx, next_tok);
        if (classified && *classified) {
            if (!add_token_to_array(result->tokens, classified)) {
                free_alias_map(&alias_map);
                free_sql_preprocessing_result(result);
                return NULL;
            }
            total_length += strlen(classified) + 1;
        }
    }

    /* Join tokens into processed query string */
    result->processed_query = (char*)palloc(total_length + 1);
    if (!result->processed_query) {
        free_alias_map(&alias_map);
        free_sql_preprocessing_result(result);
        return NULL;
    }

    char *dest = result->processed_query;
    for (size_t i = 0; i < result->tokens->count; i++) {
        const char *token = result->tokens->tokens[i];
        size_t len = strlen(token);
        memcpy(dest, token, len);
        dest += len;

        /* Add space between all tokens except before last */
        if (i < result->tokens->count - 1) {
            *dest++ = ' ';
        }
    }
    *dest = '\0';

    free_alias_map(&alias_map);
    free_output_alias_map(&output_alias_map);
    result->success = true;
    return result;
}