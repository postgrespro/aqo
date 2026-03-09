#ifndef SQL_PREPROCESSOR_H
#define SQL_PREPROCESSOR_H

#include <stddef.h>
#include <stdbool.h>

#define MAX_KEYWORDS 100
#define MAX_TOKEN_LENGTH 256
#define MAX_ALIASES 32
#define MAX_OUTPUT_ALIASES 32

typedef struct {
    char **tokens;
    size_t count;
    size_t capacity;
} TokenArray;

typedef struct {
    char *alias_name;
    int alias_number;
} AliasEntry;

typedef struct {
    AliasEntry entries[MAX_ALIASES];
    int count;
} AliasMap;

typedef struct {
    char *names[MAX_OUTPUT_ALIASES];
    int count;
} OutputAliasMap;

typedef struct {
    char *processed_query;
    TokenArray *tokens;
    bool success;
} SQLPreprocessingResult;

/* Main preprocessing function */
SQLPreprocessingResult* preprocess_sql_query(const char *sql);

/* Token array operations */
TokenArray* create_token_array(void);
bool add_token_to_array(TokenArray *arr, const char *token);
void free_token_array(TokenArray *arr);

/* Cleanup */
void free_sql_preprocessing_result(SQLPreprocessingResult *result);

#endif /* SQL_PREPROCESSOR_H */
