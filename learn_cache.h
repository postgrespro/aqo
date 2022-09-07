#ifndef LEARN_CACHE_H
#define LEARN_CACHE_H

#include "nodes/pg_list.h"

extern void lc_init(void);
extern bool lc_update_fss(uint64 fhash, int fsshash, int nrows, int ncols,
							  double **matrix, double *targets, List *relids);
extern bool lc_has_fss(uint64 fhash, int fss);
extern bool lc_load_fss(uint64 fhash, int fsshash, int ncols,
							double **matrix, double *targets, int *nrows,
							List **relids);
extern void lc_remove_fss(uint64 fhash, int fss_hash);

#endif /* LEARN_CACHE_H */
