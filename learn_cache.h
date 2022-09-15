#ifndef LEARN_CACHE_H
#define LEARN_CACHE_H

#include "nodes/pg_list.h"

#include "machine_learning.h"

extern bool aqo_learn_statement_timeout;

extern bool lc_update_fss(uint64 fhash, int fsshash, OkNNrdata *data,
						  List *relids);
extern bool lc_has_fss(uint64 fhash, int fss);
extern bool lc_load_fss(uint64 fhash, int fsshash, OkNNrdata *data,
						List **relids);
extern void lc_remove_fss(uint64 fhash, int fss_hash);
extern void lc_flush_data(void);
extern void lc_assign_hook(bool newval, void *extra);

#endif /* LEARN_CACHE_H */
