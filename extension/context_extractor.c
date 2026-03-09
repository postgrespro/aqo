#include "context_extractor.h"
#include "pg_compat.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define INITIAL_CAPACITY 32

Vocab* create_vocab(void) {
    Vocab *v = (Vocab*) palloc(sizeof(Vocab));
    if (!v) return NULL;

    v->tokens = (char**) palloc(INITIAL_CAPACITY * sizeof(char*));
    v->ids = (int*) palloc(INITIAL_CAPACITY * sizeof(int));

    if (!v->tokens || !v->ids) {
        if (v->tokens) pfree(v->tokens);
        if (v->ids) pfree(v->ids);
        pfree(v);
        return NULL;
    }

    v->count = 0;
    v->capacity = INITIAL_CAPACITY;
    return v;
}

static int find_token(Vocab *v, const char *token) {
    for (size_t i = 0; i < v->count; i++) {
        if (strcmp(v->tokens[i], token) == 0) return v->ids[i];
    }
    return -1;
}

int get_or_add_token_id(Vocab *v, const char *token) {
    if (!v || !token) return -1;

    int existing = find_token(v, token);
    if (existing != -1) return existing;

    if (v->count >= v->capacity) {
        size_t new_cap = v->capacity * 2;
        char **new_tokens = (char**) repalloc(v->tokens, sizeof(char*) * new_cap);
        int *new_ids = (int*) repalloc(v->ids, sizeof(int) * new_cap);

        if (!new_tokens || !new_ids) return -1;

        v->tokens = new_tokens;
        v->ids = new_ids;
        v->capacity = new_cap;
    }

    int new_id = (int)v->count + 1;
    v->tokens[v->count] = pstrdup(token);
    if (!v->tokens[v->count]) return -1;

    v->ids[v->count] = new_id;
    v->count++;

    return new_id;
}

void free_vocab(Vocab *v) {
    if (!v) return;
    for (size_t i = 0; i < v->count; i++) {
        pfree(v->tokens[i]);
    }
    pfree(v->tokens);
    pfree(v->ids);
    pfree(v);
}

IdSequence* tokens_to_ids(char **tokens, size_t token_count, Vocab *vocab) {
    if (!tokens || !vocab) return NULL;

    IdSequence *seq = (IdSequence*) palloc(sizeof(IdSequence));
    if (!seq) return NULL;

    seq->data = (int*) palloc(sizeof(int) * token_count);
    if (!seq->data) {
        pfree(seq);
        return NULL;
    }

    seq->count = token_count;
    for (size_t i = 0; i < token_count; i++) {
        int id = get_or_add_token_id(vocab, tokens[i]);
        if (id < 0) {
            pfree(seq->data);
            pfree(seq);
            return NULL;
        }
        seq->data[i] = id;
    }

    return seq;
}

void free_id_sequence(IdSequence *seq) {
    if (!seq) return;
    pfree(seq->data);
    pfree(seq);
}

TrainingPairArray* extract_training_pairs(IdSequence *seq, int window) {
    if (!seq || window <= 0) return NULL;

    TrainingPairArray *arr = (TrainingPairArray*) palloc(sizeof(TrainingPairArray));
    if (!arr) return NULL;

    arr->pairs = (TrainingPair*) palloc0(sizeof(TrainingPair) * seq->count);
    if (!arr->pairs) {
        pfree(arr);
        return NULL;
    }

    arr->count = seq->count;

    for (size_t i = 0; i < seq->count; i++) {
        int max_ctx = window * 2;
        int *contexts = (int*) palloc(sizeof(int) * max_ctx);
        int *rel_pos = (int*) palloc(sizeof(int) * max_ctx);

        if (!contexts || !rel_pos) {
            if (contexts) pfree(contexts);
            if (rel_pos) pfree(rel_pos);

            for (size_t t = 0; t < i; t++) {
                pfree(arr->pairs[t].contexts);
                pfree(arr->pairs[t].rel_pos);
            }
            pfree(arr->pairs);
            pfree(arr);
            return NULL;
        }

        size_t ctx_count = 0;
        int start = (int)i - window;
        int end = (int)i + window;

        if (start < 0) start = 0;
        if (end >= (int)seq->count) end = (int)seq->count - 1;

        for (int j = start; j <= end; j++) {
            if (j == (int)i) continue;
            contexts[ctx_count] = seq->data[j];
            rel_pos[ctx_count] = j - (int)i;
            ctx_count++;
        }

        arr->pairs[i].center = seq->data[i];
        arr->pairs[i].center_pos = (int)i;
        arr->pairs[i].contexts = contexts;
        arr->pairs[i].rel_pos = rel_pos;
        arr->pairs[i].context_count = ctx_count;
    }

    return arr;
}

void free_training_pairs(TrainingPairArray *arr) {
    if (!arr) return;
    for (size_t i = 0; i < arr->count; i++) {
        pfree(arr->pairs[i].contexts);
        pfree(arr->pairs[i].rel_pos);
    }
    pfree(arr->pairs);
    pfree(arr);
}

void print_training_pairs(TrainingPairArray *arr) {
    if (!arr) return;
    for (size_t i = 0; i < arr->count; i++) {
        printf("Center: %d (pos=%d) | Context: ", arr->pairs[i].center, arr->pairs[i].center_pos);
        for (size_t j = 0; j < arr->pairs[i].context_count; j++) {
            printf("%d(rel=%d) ", arr->pairs[i].contexts[j], arr->pairs[i].rel_pos[j]);
        }
        printf("\n");
    }
}