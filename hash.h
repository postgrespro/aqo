#ifndef AQO_HASH_H
#define AQO_HASH_H

#include "nodes/pg_list.h"

extern uint64 get_query_hash(Query *parse, const char *query_text);
extern bool list_member_uint64(const List *list, uint64 datum);
extern List *lappend_uint64(List *list, uint64 datum);
extern List *ldelete_uint64(List *list, uint64 datum);
extern int get_fss_for_object(List *relidslist, List *clauselist,
							  List *selectivities, int *nfeatures,
							  double **features);
extern int get_int_array_hash(int *arr, int len);
extern int get_grouped_exprs_hash(int fss, List *group_exprs);

#endif							/* AQO_HASH_H */