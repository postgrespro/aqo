#ifndef STORAGE_H
#define STORAGE_H

#include "utils/array.h"
#include "utils/dsa.h" /* Public structs have links to DSA memory blocks */

#define STAT_SAMPLE_SIZE	(20)

/*
 * Storage struct for AQO statistics
 * It is mostly needed for auto tuning feature. With auto tuning mode aqo
 * analyzes stability of last executions of the query, negative influence of
 * strong cardinality estimation on a query execution (planner bug?) and so on.
 * It can motivate aqo to suppress machine learning for this query class.
 * Also, it can be used for an analytics.
 */
typedef struct StatEntry
{
	uint64	queryid; /* The key in the hash table, should be the first field ever */

	int64	execs_with_aqo;
	int64	execs_without_aqo;

	int		cur_stat_slot;
	double	exec_time[STAT_SAMPLE_SIZE];
	double	plan_time[STAT_SAMPLE_SIZE];
	double	est_error[STAT_SAMPLE_SIZE];

	int		cur_stat_slot_aqo;
	double	exec_time_aqo[STAT_SAMPLE_SIZE];
	double	plan_time_aqo[STAT_SAMPLE_SIZE];
	double	est_error_aqo[STAT_SAMPLE_SIZE];
} StatEntry;

/*
 * Storage entry for query texts.
 * Query strings may have very different sizes. So, in hash table we store only
 * link to DSA-allocated memory.
 */
typedef struct QueryTextEntry
{
	uint64	queryid;

	/* Link to DSA-allocated momory block. Can be shared across backends */
	dsa_pointer qtext_dp;
} QueryTextEntry;

extern bool aqo_use_file_storage;

extern HTAB *stat_htab;
extern HTAB *qtexts_htab;
extern HTAB *queries_htab; /* TODO */
extern HTAB *data_htab; /* TODO */

extern StatEntry *aqo_stat_store(uint64 queryid, bool use_aqo, double plan_time,
								 double exec_time, double est_error);
extern void aqo_stat_flush(void);
extern void aqo_stat_load(void);

extern bool aqo_qtext_store(uint64 queryid, const char *query_string);
extern void aqo_qtexts_flush(void);
extern void aqo_qtexts_load(void);
/* Utility routines */
extern ArrayType *form_vector(double *vector, int nrows);

#endif /* STORAGE_H */
