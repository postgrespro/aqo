#ifndef W2V_EMBEDDING_EXTRACTOR_H
#define W2V_EMBEDDING_EXTRACTOR_H

#include <stdbool.h>

bool w2v_inference_init(const char *v_path, const char *e_path, int k, int d);
void w2v_inference_cleanup(void);
int extractor_get_word_id(const char *word);
const float* extractor_get_word_embedding(int word_id);

int w2v_inference_get_dim(void);
bool w2v_inference_is_ready(void);

#endif