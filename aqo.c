/*
 * aqo.c
 *		Adaptive query optimization extension
 *
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/aqo.c
 */

#include "postgres.h"

#include "aqo.h"

#include "access/relation.h"
#include "access/table.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "utils/selfuncs.h"

#include "aqo.h"
#include "aqo_shared.h"
#include "cardinality_hooks.h"
#include "path_utils.h"
#include "postmaster/bgworker.h"
#include "preprocessing.h"
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
int			aqo_stat_size = STAT_SAMPLE_SIZE;
int			auto_tuning_window_size = 5;
double		auto_tuning_exploration = 0.1;
int			auto_tuning_max_iterations = 50;
int			auto_tuning_infinite_loop = 8;

/* stat_size > infinite_loop + window_size + 3 is required for auto_tuning*/

/* Machine learning parameters */

/* The number of nearest neighbors which will be chosen for ML-operations */
int			aqo_k;
double		log_selectivity_lower_bound = -30;

bool		cleanup_bgworker = false;

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

/* Additional plan info */
int njoins;

/* Saved hook values */
post_parse_analyze_hook_type				prev_post_parse_analyze_hook;
planner_hook_type							prev_planner_hook;
ExecutorStart_hook_type						prev_ExecutorStart_hook;
ExecutorRun_hook_type						prev_ExecutorRun;
ExecutorEnd_hook_type						prev_ExecutorEnd_hook;
set_baserel_rows_estimate_hook_type			prev_set_foreign_rows_estimate_hook;
set_baserel_rows_estimate_hook_type			prev_set_baserel_rows_estimate_hook;
get_parameterized_baserel_size_hook_type	prev_get_parameterized_baserel_size_hook;
set_joinrel_size_estimates_hook_type		prev_set_joinrel_size_estimates_hook;
get_parameterized_joinrel_size_hook_type	prev_get_parameterized_joinrel_size_hook;
ExplainOnePlan_hook_type					prev_ExplainOnePlan_hook;
ExplainOneNode_hook_type					prev_ExplainOneNode_hook;
static shmem_request_hook_type				prev_shmem_request_hook = NULL;
object_access_hook_type						prev_object_access_hook;

PGDLLEXPORT void aqo_bgworker_cleanup(Datum main_arg);
static void aqo_bgworker_startup(void);

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
	}
}

/*
 * Requests any additional shared memory required for aqo.
 */
static void
aqo_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(aqo_memsize());
}

/*
 * Entry point for CleanupWorker's process.
 */
void
aqo_bgworker_cleanup(Datum main_arg)
{
	int	fs_num;
	int	fss_num;

	cleanup_aqo_database(true, &fs_num, &fss_num);
}

/*
 * Object access hook
 */
static void
aqo_drop_access_hook(ObjectAccessType access,
					 Oid classId,
					 Oid objectId,
					 int subId,
					 void *arg)
{
	if (prev_object_access_hook)
		(*prev_object_access_hook) (access, classId, objectId, subId, arg);

	if (access == OAT_DROP && cleanup_bgworker)
	{
		MemoryContext old_ctx;
		int status = BGWH_STOPPED;
		pid_t pid;

		old_ctx = MemoryContextSwitchTo(AQOTopMemCtx);
		LWLockAcquire(&aqo_state->lock, LW_EXCLUSIVE);
		if (aqo_state->bgw_handle != NULL)
		{
			status = GetBackgroundWorkerPid(aqo_state->bgw_handle, &pid);
		}
		LWLockRelease(&aqo_state->lock);
		if (status != BGWH_STARTED)
		{
			aqo_bgworker_startup();
		}
		MemoryContextSwitchTo(old_ctx);
	}
}

static void
aqo_bgworker_startup(void)
{
	BackgroundWorker		worker;
	BackgroundWorkerHandle	*handle;
	BgwHandleStatus			status;
	pid_t					pid;

	MemSet(&worker, 0, sizeof(worker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_extra[0] = 0;
	memcpy(worker.bgw_function_name, "aqo_bgworker_cleanup", 21);
	memcpy(worker.bgw_library_name, "aqo", 4);
	memcpy(worker.bgw_name, "aqo cleanup", 12);

	/* must set notify PID to wait for startup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(NOTICE,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("could not register background process"),
				 errhint("You might need to increase max_worker_processes.")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		ereport(NOTICE,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));

	LWLockAcquire(&aqo_state->lock, LW_EXCLUSIVE);
	aqo_state->bgw_handle = handle;
	LWLockRelease(&aqo_state->lock);
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
							0, INT_MAX,
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
							PGC_SUSET,
							0,
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

	DefineCustomBoolVariable("aqo.cleanup_bgworker",
							 "Enable bgworker which responsible for doing cleanup after drop",
							 NULL,
							 &cleanup_bgworker,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL
	);

	DefineCustomIntVariable("aqo.min_neighbors_for_predicting",
							"Set how many neighbors the cardinality prediction will be calculated",
							NULL,
							&aqo_k,
							3, 1, INT_MAX / 1000,
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

	prev_shmem_startup_hook						= shmem_startup_hook;
	shmem_startup_hook							= aqo_init_shmem;
	prev_planner_hook							= planner_hook;
	planner_hook								= aqo_planner;
	prev_ExecutorStart_hook						= ExecutorStart_hook;
	ExecutorStart_hook							= aqo_ExecutorStart;
	prev_ExecutorRun							= ExecutorRun_hook;
	ExecutorRun_hook							= aqo_ExecutorRun;
	prev_ExecutorEnd_hook						= ExecutorEnd_hook;
	ExecutorEnd_hook							= aqo_ExecutorEnd;

	/* Cardinality prediction hooks. */
	prev_set_baserel_rows_estimate_hook			= set_baserel_rows_estimate_hook;
	set_foreign_rows_estimate_hook				= aqo_set_baserel_rows_estimate;
	set_baserel_rows_estimate_hook				= aqo_set_baserel_rows_estimate;
	prev_get_parameterized_baserel_size_hook	= get_parameterized_baserel_size_hook;
	get_parameterized_baserel_size_hook			= aqo_get_parameterized_baserel_size;
	prev_set_joinrel_size_estimates_hook		= set_joinrel_size_estimates_hook;
	set_joinrel_size_estimates_hook				= aqo_set_joinrel_size_estimates;
	prev_get_parameterized_joinrel_size_hook	= get_parameterized_joinrel_size_hook;
	get_parameterized_joinrel_size_hook			= aqo_get_parameterized_joinrel_size;
	prev_estimate_num_groups_hook				= estimate_num_groups_hook;
	estimate_num_groups_hook					= aqo_estimate_num_groups_hook;
	parampathinfo_postinit_hook					= ppi_hook;

	prev_create_plan_hook						= create_plan_hook;
	create_plan_hook							= aqo_create_plan_hook;

	/* Service hooks. */
	prev_ExplainOnePlan_hook					= ExplainOnePlan_hook;
	ExplainOnePlan_hook							= print_into_explain;
	prev_ExplainOneNode_hook					= ExplainOneNode_hook;
	ExplainOneNode_hook							= print_node_explain;

	prev_create_upper_paths_hook				= create_upper_paths_hook;
	create_upper_paths_hook						= aqo_store_upper_signature_hook;

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = aqo_shmem_request;

	prev_object_access_hook						= object_access_hook;
	object_access_hook							= aqo_drop_access_hook;

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
