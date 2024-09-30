#ifndef STORAGE_H
#define STORAGE_H

#include "nodes/pg_list.h"
#include "utils/array.h"
#include "utils/dsa.h" /* Public structs have links to DSA memory blocks */

#include "aqo.h"
#include "machine_learning.h"

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
 * Auxiliary struct, used for passing arguments
 * to aqo_stat_store() function.
 */
typedef struct AqoStatArgs
{
	int64	execs_with_aqo;
	int64	execs_without_aqo;

	int		cur_stat_slot;
	double	*exec_time;
	double	*plan_time;
	double	*est_error;

	int		cur_stat_slot_aqo;
	double	*exec_time_aqo;
	double	*plan_time_aqo;
	double	*est_error_aqo;
} AqoStatArgs;

/*
 * Storage entry for query texts.
 * Query strings may have very different sizes. So, in hash table we store only
 * link to DSA-allocated memory.
 */
typedef struct QueryTextEntry
{
	uint64	queryid;

	/* Link to DSA-allocated memory block. Can be shared across backends */
	dsa_pointer qtext_dp;
} QueryTextEntry;

typedef struct data_key
{
	uint64	fs;
	int64	fss; /* just for alignment */
} data_key;

typedef struct DataEntry
{
	data_key key;

	/* defines a size and data placement in the DSA memory block */
	int cols; /* aka nfeatures */
	int rows; /* aka number of equations */
	int nrels;

	/*
	 * Link to DSA-allocated memory block. Can be shared across backends.
	 * Contains:
	 * matrix[][], targets[], reliability[], oids.
	 */
	dsa_pointer data_dp;
} DataEntry;

typedef struct QueriesEntry
{
	uint64	queryid;

	uint64	fs;
	bool	learn_aqo;
	bool	use_aqo;
	bool	auto_tuning;

	int64	smart_timeout;
	int64	count_increase_timeout;
} QueriesEntry;

/*
 * Auxiliary struct, used for passing arg NULL signs
 * to aqo_queries_store() function.
 */
typedef struct AqoQueriesNullArgs
{
	bool	fs_is_null;
	bool	learn_aqo_is_null;
	bool	use_aqo_is_null;
	bool	auto_tuning_is_null;
	int64	smart_timeout;
	int64	count_increase_timeout;
} AqoQueriesNullArgs;

/*
 * Used for internal aqo_queries_store() calls.
 * No NULL arguments expected in this case.
 */
extern AqoQueriesNullArgs aqo_queries_nulls;

extern int querytext_max_size;
extern int dsm_size_max;

extern HTAB *stat_htab;
extern HTAB *qtexts_htab;
extern HTAB *queries_htab; /* TODO */
extern HTAB *data_htab; /* TODO */

extern StatEntry *aqo_stat_store(uint64 queryid, bool use_aqo,
								 AqoStatArgs *stat_arg, bool append_mode);
extern void aqo_stat_flush(void);
extern void aqo_stat_load(void);

extern bool aqo_qtext_store(uint64 queryid, const char *query_string, bool *dsa_valid);
extern void aqo_qtexts_flush(void);
extern void aqo_qtexts_load(void);

extern bool aqo_data_store(uint64 fs, int fss, AqoDataArgs *data,
						   List *reloids);
extern bool load_aqo_data(uint64 fs, int fss, OkNNrdata *data, bool wideSearch);
extern void aqo_data_flush(void);
extern void aqo_data_load(void);

extern bool aqo_queries_find(uint64 queryid, QueryContextData *ctx);
extern bool aqo_queries_store(uint64 queryid, uint64 fs, bool learn_aqo,
							  bool use_aqo, bool auto_tuning,
							  AqoQueriesNullArgs *null_args);
extern void aqo_queries_flush(void);
extern void aqo_queries_load(void);

extern void check_dsa_file_size(void);
/*
 * Machinery for deactivated queries cache.
 * TODO: Should live in a custom memory context
 */
extern void init_deactivated_queries_storage(void);
extern bool query_is_deactivated(uint64 query_hash);
extern void add_deactivated_query(uint64 query_hash);

/* Storage interaction */
extern bool update_fss_ext(uint64 fs, int fss, OkNNrdata *data, List *reloids);

extern bool update_query_timeout(uint64 queryid, int64 smart_timeout);

#endif /* STORAGE_H */
