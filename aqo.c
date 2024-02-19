/*
 * aqo.c
 *		Adaptive query optimization extension
 *
 * Copyright (c) 2016-2023, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/aqo.c
 */

#include "postgres.h"

#include "access/relation.h"
#include "access/table.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "utils/selfuncs.h"

#include "aqo.h"
#include "aqo_shared.h"
#include "path_utils.h"
#include "storage.h"


PG_MODULE_MAGIC;

void _PG_init(void);

#define AQO_MODULE_MAGIC	(1234)

/* Strategy of determining feature space for new queries. */
int		aqo_mode = AQO_MODE_CONTROLLED;
bool	force_collect_stat;
bool	aqo_predict_with_few_neighbors;
int 	aqo_statement_timeout;

/*
 * Show special info in EXPLAIN mode.
 *
 * aqo_show_hash - show query class (hash) and a feature space value (hash)
 * of each plan node. This is instance-dependent value and can't be used
 * in regression and TAP tests.
 *
 * aqo_show_details - show AQO settings for this class and prediction
 * for each plan node.
 */
bool	aqo_show_hash;
bool	aqo_show_details;
bool	change_flex_timeout;

/* GUC variables */
static const struct config_enum_entry format_options[] = {
	{"intelligent", AQO_MODE_INTELLIGENT, false},
	{"forced", AQO_MODE_FORCED, false},
	{"controlled", AQO_MODE_CONTROLLED, false},
	{"learn", AQO_MODE_LEARN, false},
	{"frozen", AQO_MODE_FROZEN, false},
	{"disabled", AQO_MODE_DISABLED, false},
	{NULL, 0, false}
};

/* Parameters of autotuning */
int			auto_tuning_window_size = 5;
double		auto_tuning_exploration = 0.1;
int			auto_tuning_max_iterations = 50;
int			auto_tuning_infinite_loop = 8;

/* stat_size > infinite_loop + window_size + 3 is required for auto_tuning*/

/* Machine learning parameters */

/* The number of nearest neighbors which will be chosen for ML-operations */
int			aqo_k;
double		log_selectivity_lower_bound = -30;

/*
 * Currently we use it only to store query_text string which is initialized
 * after a query parsing and is used during the query planning.
 */

QueryContextData	query_context;

MemoryContext		AQOTopMemCtx = NULL;

/* Is released at the end of transaction */
MemoryContext		AQOCacheMemCtx = NULL;

/* Is released at the end of planning */
MemoryContext 		AQOPredictMemCtx = NULL;

/* Is released at the end of learning */
MemoryContext 		AQOLearnMemCtx = NULL;

/* Is released at the end of load/store routines */
MemoryContext 		AQOStorageMemCtx = NULL;

/* Additional plan info */
int njoins;

/*****************************************************************************
 *
 *	CREATE/DROP EXTENSION FUNCTIONS
 *
 *****************************************************************************/

static void
aqo_free_callback(ResourceReleasePhase phase,
					 bool isCommit,
					 bool isTopLevel,
					 void *arg)
{
	if (phase != RESOURCE_RELEASE_AFTER_LOCKS)
		return;

	if (isTopLevel)
	{
		MemoryContextReset(AQOCacheMemCtx);
		cur_classes = NIL;
		aqo_eclass_collector = NIL;
	}
}

void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries. If not, report an ERROR.
	 */
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("AQO module could be loaded only on startup."),
				 errdetail("Add 'aqo' into the shared_preload_libraries list.")));

	/*
	 * Inform the postmaster that we want to enable query_id calculation if
	 * compute_query_id is set to auto.
	 */
	EnableQueryId();

	DefineCustomEnumVariable("aqo.mode",
							 "Mode of aqo usage.",
							 NULL,
							 &aqo_mode,
							 AQO_MODE_CONTROLLED,
							 format_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL
	);

	DefineCustomBoolVariable(
							 "aqo.force_collect_stat",
							 "Collect statistics at all AQO modes",
							 NULL,
							 &force_collect_stat,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL
	);

	DefineCustomBoolVariable(
							 "aqo.show_hash",
							 "Show query and node hash on explain.",
							 "Hash value depend on each instance and is not good to enable it in regression or TAP tests.",
							 &aqo_show_hash,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL
	);

	DefineCustomBoolVariable(
							 "aqo.show_details",
							 "Show AQO state on a query.",
							 NULL,
							 &aqo_show_details,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL
	);

	DefineCustomBoolVariable(
							 "aqo.learn_statement_timeout",
							 "Learn on a plan interrupted by statement timeout.",
							 "ML data stored in a backend cache, so it works only locally.",
							 &aqo_learn_statement_timeout,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL
	);

	DefineCustomBoolVariable(
							 "aqo.wide_search",
							 "Search ML data in neighbour feature spaces.",
							 NULL,
							 &use_wide_search,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL
	);

	DefineCustomIntVariable("aqo.join_threshold",
							"Sets the threshold of number of JOINs in query beyond which AQO is used.",
							NULL,
							&aqo_join_threshold,
							3,
							0, INT_MAX / 1000,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL
	);

	DefineCustomIntVariable("aqo.fs_max_items",
							"Max number of feature spaces that AQO can operate with.",
							NULL,
							&fs_max_items,
							10000,
							1, INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL
	);

	DefineCustomIntVariable("aqo.fss_max_items",
							"Max number of feature subspaces that AQO can operate with.",
							NULL,
							&fss_max_items,
							100000,
							0, INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL
	);

	DefineCustomIntVariable("aqo.querytext_max_size",
							"Query max size in aqo_query_texts.",
							NULL,
							&querytext_max_size,
							1000,
							1, INT_MAX,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL
	);

	DefineCustomIntVariable("aqo.dsm_size_max",
							"Maximum size of dynamic shared memory which AQO could allocate to store learning data.",
							NULL,
							&dsm_size_max,
							100,
							0, INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_MB,
							NULL,
							NULL,
							NULL
	);
	DefineCustomIntVariable("aqo.statement_timeout",
							"Time limit on learning.",
							NULL,
							&aqo_statement_timeout,
							0,
							0, INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("aqo.min_neighbors_for_predicting",
							"Set how many neighbors the cardinality prediction will be calculated",
							NULL,
							&aqo_k,
							3,
							1, INT_MAX / 1000,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("aqo.predict_with_few_neighbors",
							"Establish the ability to make predictions with fewer neighbors than were found.",
							 NULL,
							 &aqo_predict_with_few_neighbors,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	aqo_shmem_init();
	aqo_preprocessing_init();
	aqo_postprocessing_init();
	aqo_cardinality_hooks_init();
	aqo_path_utils_init();

	init_deactivated_queries_storage();

	/*
	 * Create own Top memory Context for reporting AQO memory in the future.
	 */
	AQOTopMemCtx = AllocSetContextCreate(TopMemoryContext,
											 "AQOTopMemoryContext",
											 ALLOCSET_DEFAULT_SIZES);
	/*
	 * AQO Cache Memory Context containe environment data.
	 */
	AQOCacheMemCtx = AllocSetContextCreate(AQOTopMemCtx,
											 "AQOCacheMemCtx",
											 ALLOCSET_DEFAULT_SIZES);

	/*
	 * AQOPredictMemoryContext save necessary information for making predict of plan nodes
	 * and clean up in the execution stage of query.
	 */
	AQOPredictMemCtx = AllocSetContextCreate(AQOTopMemCtx,
											 "AQOPredictMemoryContext",
											 ALLOCSET_DEFAULT_SIZES);
	/*
	 * AQOLearnMemoryContext save necessary information for writing down to AQO knowledge table
	 * and clean up after doing this operation.
	 */
	AQOLearnMemCtx = AllocSetContextCreate(AQOTopMemCtx,
											 "AQOLearnMemoryContext",
											 ALLOCSET_DEFAULT_SIZES);
	/*
	 * AQOStorageMemoryContext containe data for load/store routines.
	 */
	AQOStorageMemCtx = AllocSetContextCreate(AQOTopMemCtx,
											 "AQOStorageMemoryContext",
											 ALLOCSET_DEFAULT_SIZES);
	RegisterResourceReleaseCallback(aqo_free_callback, NULL);
	RegisterAQOPlanNodeMethods();

	MarkGUCPrefixReserved("aqo");
}

/*
 * AQO is really needed for any activity?
 */
bool
IsQueryDisabled(void)
{
	if (!query_context.learn_aqo && !query_context.use_aqo &&
		!query_context.auto_tuning && !query_context.collect_stat &&
		!query_context.adding_query && !query_context.explain_only &&
		INSTR_TIME_IS_ZERO(query_context.start_planning_time) &&
		query_context.planning_time < 0.)
		return true;

	return false;
}

PG_FUNCTION_INFO_V1(invalidate_deactivated_queries_cache);

/*
 * Clears the cache of deactivated queries if the user changed aqo_queries
 * manually.
 */
Datum
invalidate_deactivated_queries_cache(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(NULL);
}
