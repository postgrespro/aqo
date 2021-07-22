#ifndef AQO_HASH_H
#define AQO_HASH_H

#include "nodes/pg_list.h"

extern int get_int_array_hash(int *arr, int len);
extern int get_grouped_exprs_hash(int fss, List *group_exprs);

#endif							/* AQO_HASH_H */