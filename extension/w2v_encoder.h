#ifndef W2V_ENCODER_H
#define W2V_ENCODER_H

typedef struct {
    float *aggregate_vector;
    int num_words;
    int word_dim;
} EncodedQuery;

EncodedQuery* encode_sql_query(const char *sql_query, float sigma);
void free_encoded_query(EncodedQuery *eq);

#endif