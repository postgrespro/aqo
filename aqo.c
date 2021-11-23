/*
 * aqo.c
 *		Adaptive query optimization extension
 *
 * Copyright (c) 2016-2021, Postgres Professional
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
#include "cardinality_hooks.h"
#include "ignorance.h"
#include "path_utils.h"
#include "preprocessing.h"


PG_MODULE_MAGIC;

void _PG_init(void);

#define AQO_MODULE_MAGIC	(1234)

/* Strategy of determining feature space for new queries. */
int		aqo_mode;
bool	aqo_enabled = false; /* Signals that CREATE EXTENSION have executed and
								all extension tables is ready for use. */
bool	force_collect_stat;

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
int			aqo_stat_size = 20;
int			auto_tuning_window_size = 5;
double		auto_tuning_exploration = 0.1;
int			auto_tuning_max_iterations = 50;
int			auto_tuning_infinite_loop = 8;

/* stat_size > infinite_loop + window_size + 3 is required for auto_tuning*/

/* Machine learning parameters */

/*
 * Defines where we do not perform learning procedure
 */
const double	object_selection_prediction_threshold = 0.3;

/*
 * This parameter tell us that the new learning sample object has very small
 * distance from one whose features stored in matrix already.
 * In this case we will not to add new line in matrix, but will modify this
 * nearest neighbor features and cardinality with linear smoothing by
 * learning_rate coefficient.
 */
const double	object_selection_threshold = 0.1;
const double	learning_rate = 1e-1;

/* The number of nearest neighbors which will be chosen for ML-operations */
int			aqo_k = 3;
double		log_selectivity_lower_bound = -30;

/*
 * Currently we use it only to store query_text string which is initialized
 * after a query parsing and is used during the query planning.
 */
MemoryContext		AQOMemoryContext;
QueryContextData	query_context;
/* Additional plan info */
int njoins;

/* Saved hook values */
post_parse_analyze_hook_type				prev_post_parse_analyze_hook;
planner_hook_type							prev_planner_hook;
ExecutorStart_hook_type						prev_ExecutorStart_hook;
ExecutorEnd_hook_type						prev_ExecutorEnd_hook;
set_baserel_rows_estimate_hook_type			prev_set_foreign_rows_estimate_hook;
set_baserel_rows_estimate_hook_type			prev_set_baserel_rows_estimate_hook;
get_parameterized_baserel_size_hook_type	prev_get_parameterized_baserel_size_hook;
set_joinrel_size_estimates_hook_type		prev_set_joinrel_size_estimates_hook;
get_parameterized_joinrel_size_hook_type	prev_get_parameterized_joinrel_size_hook;
create_plan_hook_type						prev_create_plan_hook;
ExplainOnePlan_hook_type					prev_ExplainOnePlan_hook;
ExplainOneNode_hook_type					prev_ExplainOneNode_hook;

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
		list_free_deep(cur_classes);
		cur_classes = NIL;
	}
}

void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, report an ERROR.
	 */
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("AQO module could be loaded only on startup."),
				 errdetail("Add 'aqo' into the shared_preload_libraries list.")));
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
							 "aqo.log_ignorance",
							 "Log in a special table all feature spaces for which the AQO prediction was not successful.",
							 NULL,
							 &aqo_log_ignorance,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 set_ignorance,
							 NULL
	);

	prev_planner_hook							= planner_hook;
	planner_hook								= aqo_planner;
	prev_ExecutorStart_hook						= ExecutorStart_hook;
	ExecutorStart_hook							= aqo_ExecutorStart;
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

	init_deactivated_queries_storage();
	AQOMemoryContext = AllocSetContextCreate(TopMemoryContext,
											 "AQOMemoryContext",
											 ALLOCSET_DEFAULT_SIZES);
	RegisterResourceReleaseCallback(aqo_free_callback, NULL);
	RegisterAQOPlanNodeMethods();
}

PG_FUNCTION_INFO_V1(invalidate_deactivated_queries_cache);

/*
 * Clears the cache of deactivated queries if the user changed aqo_queries
 * manually.
 */
Datum
invalidate_deactivated_queries_cache(PG_FUNCTION_ARGS)
{
	fini_deactivated_queries_storage();
	init_deactivated_queries_storage();
	PG_RETURN_POINTER(NULL);
}

/*
 * Return AQO schema's Oid or InvalidOid if that's not possible.
 */
Oid
get_aqo_schema(void)
{
	Oid				result;
	Relation		rel;
	SysScanDesc		scandesc;
	HeapTuple		tuple;
	ScanKeyData		entry[1];
	Oid				ext_oid;

	/* It's impossible to fetch pg_aqo's schema now */
	if (!IsTransactionState())
		return InvalidOid;

	ext_oid = get_extension_oid("aqo", true);
	if (ext_oid == InvalidOid)
		return InvalidOid; /* exit if pg_aqo does not exist */

	ScanKeyInit(&entry[0],
#if PG_VERSION_NUM >= 120000
				Anum_pg_extension_oid,
#else
				ObjectIdAttributeNumber,
#endif
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_oid));

	rel = relation_open(ExtensionRelationId, AccessShareLock);
	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  NULL, 1, entry);
	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);
	relation_close(rel, AccessShareLock);
	return result;
}

/*
 * Init userlock
 */
void
init_lock_tag(LOCKTAG *tag, uint32 key1, uint32 key2)
{
	tag->locktag_field1 = AQO_MODULE_MAGIC;
	tag->locktag_field2 = key1;
	tag->locktag_field3 = key2;
	tag->locktag_field4 = 0;
	tag->locktag_type = LOCKTAG_USERLOCK;
	tag->locktag_lockmethodid = USER_LOCKMETHOD;
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
