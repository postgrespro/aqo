#ifndef CONTEXT_EXTRACTOR_H
#define CONTEXT_EXTRACTOR_H

#include <stddef.h>

typedef struct {
    char **tokens;
    int *ids;
    size_t count;
    size_t capacity;
} Vocab;

typedef struct {
    int *data;
    size_t count;
} IdSequence;

typedef struct {
    int center;
    int center_pos;
    int *contexts;
    int *rel_pos;
    size_t context_count;
} TrainingPair;

typedef struct {
    TrainingPair *pairs;
    size_t count;
} TrainingPairArray;

Vocab* create_vocab(void);
int get_or_add_token_id(Vocab *vocab, const char *token);
void free_vocab(Vocab *vocab);

IdSequence* tokens_to_ids(char **tokens, size_t token_count, Vocab *vocab);
void free_id_sequence(IdSequence *seq);

TrainingPairArray* extract_training_pairs(IdSequence *seq, int window);
void free_training_pairs(TrainingPairArray *arr);

void print_training_pairs(TrainingPairArray *arr);

#endif
