#ifndef AQO_HASH_H
#define AQO_HASH_H

#include "nodes/pg_list.h"

extern bool list_member_uint64(const List *list, uint64 datum);
extern List *list_copy_uint64(List *list);
extern List *lappend_uint64(List *list, uint64 datum);
extern List *ldelete_uint64(List *list, uint64 datum);
extern int get_fss_for_object(List *relsigns, List *clauselist,
							  List *selectivities, int *nfeatures,
							  double **features);
extern int get_int_array_hash(int *arr, int len);
extern int get_grouped_exprs_hash(int fss, List *group_exprs);

#endif							/* AQO_HASH_H */