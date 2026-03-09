#include "../utils/pg_compat.h"
#include "w2v_encoder.h"
#include "w2v_embedding_extractor.h"
#include "../utils/sql_preprocessor.h"
#include <math.h>
#include <string.h>
//  w_j = e^(-(cw - j)^2 / (2 * sigma^2))
static float calculate_positional_weight(int current_pos, float center_pos, float sigma) {
    float diff = center_pos - (float)current_pos;
    return expf(-(diff * diff) / (2.0f * sigma * sigma));
}

EncodedQuery* encode_sql_query(const char *sql, float sigma) {
    if (!extractor_is_loaded()) return NULL;

    SQLPreprocessingResult *prep = preprocess_sql_query(sql);
    if (!prep || !prep->tokens || prep->tokens->count == 0) return NULL;

    int D = extractor_get_dim();
    size_t M = prep->tokens->count;
    float cw = (M - 1) / 2.0f;

    float *query_vector = palloc0(D * sizeof(float));
    float weight_sum = 0.0f;
    int valid_words = 0;

    for (size_t j = 0; j < M; j++) {
        int wid = extractor_get_word_id(prep->tokens->tokens[j]);
        if (wid < 0) continue;

        const float *emb = extractor_get_word_embedding(wid);
        if (!emb) continue;

        bool is_clean = true;
        for (int d = 0; d < D; d++) {
            if (!isfinite(emb[d])) {
                is_clean = false;
                break;
            }
        }
        if (!is_clean) continue;

        float w_j = calculate_positional_weight((int)j, cw, sigma);

        for (int d = 0; d < D; d++) {
            query_vector[d] += w_j * emb[d];
        }
        weight_sum += w_j;
        valid_words++;
    }

    if (weight_sum > 0.0f) {
        for (int d = 0; d < D; d++) {
            query_vector[d] /= weight_sum;
        }
    }

    if (valid_words == 0) {
        pfree(query_vector);
        if (prep) free_sql_preprocessing_result(prep);
        return NULL;
    }

    EncodedQuery *res = palloc(sizeof(EncodedQuery));
    res->aggregate_vector = query_vector;
    res->num_words = valid_words;
    res->word_dim = D;

    free_sql_preprocessing_result(prep);
    return res;
}

void free_encoded_query(EncodedQuery *eq) {
    if (eq) {
        if (eq->aggregate_vector) pfree(eq->aggregate_vector);
        pfree(eq);
    }
}