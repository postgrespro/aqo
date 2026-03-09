#ifndef W2V_ENCODER_H
#define W2V_ENCODER_H

typedef struct {
    float *aggregate_vector;
    int num_words;
    int word_dim;
} W2VEmbeddingResult;

W2VEmbeddingResult* w2v_extract_sql_embedding(const char *sql_query, float sigma);
void w2v_free_embedding_result(W2VEmbeddingResult *eq);

#endif