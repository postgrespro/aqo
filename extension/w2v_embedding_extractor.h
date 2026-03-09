#ifndef W2V_EMBEDDING_EXTRACTOR_H
#define W2V_EMBEDDING_EXTRACTOR_H

#include <stdbool.h>

bool init_embedding_extractor(const char *v_path, const char *e_path, int k, int d);
void free_embedding_extractor(void);
int extractor_get_word_id(const char *word);
const float* extractor_get_word_embedding(int word_id);

int extractor_get_dim(void);
bool extractor_is_loaded(void);

#endif