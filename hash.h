#ifndef AQO_HASH_H
#define AQO_HASH_H

#include "nodes/pg_list.h"

extern int get_query_hash(Query *parse, const char *query_text);
extern int get_fss_for_object(List *relidslist, List *clauselist,
							  List *selectivities, int *nfeatures,
							  double **features);
extern int get_int_array_hash(int *arr, int len);
extern int get_grouped_exprs_hash(int fss, List *group_exprs);

#endif							/* AQO_HASH_H */