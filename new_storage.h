#ifndef NEW_STORAGE_H
#define NEW_STORAGE_H

#include "postgres.h"

extern void makeAqoDir(void);
extern void file_add_query_text(int qhash, const char *query_string);
extern bool use_file_storage;

#endif /* NEW_STORAGE_H */