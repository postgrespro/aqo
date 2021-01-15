/*
 * aqo.c
 *		Adaptive query optimization extension
 *
 * Copyright (c) 2016-2020, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/aqo.c
 */

#include "aqo.h"

PG_MODULE_MAGIC;

void _PG_init(void);


/* Strategy of determining feature space for new queries. */
int		aqo_mode;
bool	force_collect_stat;

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

char				*query_text = NULL;

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
copy_generic_path_info_hook_type			prev_copy_generic_path_info_hook;
ExplainOnePlan_hook_type					prev_ExplainOnePlan_hook;

/*****************************************************************************
 *
 *	CREATE/DROP EXTENSION FUNCTIONS
 *
 *****************************************************************************/

void
_PG_init(void)
{
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
							 NULL);

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

	prev_planner_hook							= planner_hook;
	planner_hook								= aqo_planner;
	prev_post_parse_analyze_hook				= post_parse_analyze_hook;
	post_parse_analyze_hook						= get_query_text;
	prev_ExecutorStart_hook						= ExecutorStart_hook;
	ExecutorStart_hook							= aqo_ExecutorStart;
	prev_ExecutorEnd_hook						= ExecutorEnd_hook;
	ExecutorEnd_hook							= aqo_ExecutorEnd;
	prev_set_baserel_rows_estimate_hook			= set_baserel_rows_estimate_hook;
	set_foreign_rows_estimate_hook				= aqo_set_baserel_rows_estimate;
	set_baserel_rows_estimate_hook				= aqo_set_baserel_rows_estimate;
	prev_get_parameterized_baserel_size_hook	= get_parameterized_baserel_size_hook;
	get_parameterized_baserel_size_hook			= aqo_get_parameterized_baserel_size;
	prev_set_joinrel_size_estimates_hook		= set_joinrel_size_estimates_hook;
	set_joinrel_size_estimates_hook				= aqo_set_joinrel_size_estimates;
	prev_get_parameterized_joinrel_size_hook	= get_parameterized_joinrel_size_hook;
	get_parameterized_joinrel_size_hook			= aqo_get_parameterized_joinrel_size;
	prev_copy_generic_path_info_hook			= copy_generic_path_info_hook;
	copy_generic_path_info_hook					= aqo_copy_generic_path_info;
	prev_ExplainOnePlan_hook					= ExplainOnePlan_hook;
	ExplainOnePlan_hook							= print_into_explain;
	parampathinfo_postinit_hook					= ppi_hook;

	init_deactivated_queries_storage();
	AQOMemoryContext = AllocSetContextCreate(TopMemoryContext,
											 "AQOMemoryContext",
											 ALLOCSET_DEFAULT_SIZES);
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
