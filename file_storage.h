#ifndef FILE_STORAGE_H
#define FILE_STORAGE_H

#include "postgres.h"

extern void makeAqoDir(void);
extern void file_add_query_text(uint64 qhash, const char *query_string);
extern bool use_file_storage;

#endif /* FILE_STORAGE_H */