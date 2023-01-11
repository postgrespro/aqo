/*
 * aqo.h
 *		Adaptive query optimization extension
 *
 * Adaptive query optimization is the kind of query optimization in which
 * the execution statistics from previously executed queries is used.
 * Adaptive query optimization extends standard PostgreSQL cost-based query
 * optimization model.
 * This extension uses machine learning model built over the collected
 * statistics to improve cardinality estimations.
 *
 * The extension organized as follows.
 *
 * Query type or query hash is an integer number. Queries belongs to the same
 * type if they have the same structure, i. e. their difference lies only in
 * their constant values.
 * The settings of handling for query types are contained in aqo_queries table.
 * Examples of query texts for different query types are available in
 * aqo_query_texts table.
 * Query types are linked to feature spaces.
 *
 * Feature space is the set of machine learning models and their settings
 * used for cardinality prediction. The statistics of query types from one
 * feature space will interact. This interaction may be constructive or
 * destructive, which leads to performance improvement or performance
 * degradation respectively.
 * Feature spaces are described by their hashes (an integer value).
 *
 * This extension presents four default modes:
 * "intelligent" mode tries to automatically tune AQO settings for the current
 * workload. It creates separate feature space for each new type of query
 * and then tries to improve the performance of such query type execution.
 * The automatic tuning may be manually deactivated for some queries.
 * "learn" mode creates separate feature space and enabled aqo learning and
 * usage for each new type of query. In general it is similar to "intelligent"
 * mode, but without auto_tuning setting enabled by default.
 * "forced" mode makes no difference between query types and use AQO for them
 * all in the similar way. It considers each new query type as linked to special
 * feature space called COMMON with hash 0.
 * "controlled" mode ignores unknown query types. In this case AQO is completely
 * configured manually by user.
 * "disabled" mode ignores all queries.
 * Current mode is stored in aqo.mode variable.
 *
 * User can manually set up his own feature space configuration
 * for query types by changing settings in table aqo_queries.
 *
 * Module preprocessing.c determines how to handle the given query.
 * This includes following questions: whether to use AQO for this query,
 * whether to use execution statistics of this query to update machine
 * learning models, to what feature space the query belongs to, and whether
 * this query allows using intelligence autotuning for three previous questions.
 * This data is stored in aqo_queries table. Also this module links
 * new query types to their feature spaces according to aqo.mode.
 *
 * If it is supposed to use AQO for given type of query, the extension hooks
 * cardinality estimation functions in PostgreSQL. If the necessary statistics
 * for cardinality predictions using machine learning method is available,
 * the extension performs the prediction and returns its value. Otherwise it
 * refused to predict and returns control to standard PostgreSQL cardinality
 * estimator.
 * Modules cardinality_hooks.c and cardinality_estimation.c are responsible
 * for this part of work.
 *
 * If it is supposed to use execution statistics of given query for learning
 * models in AQO, the extension sets flag before execution to collect rows
 * statistics. After query execution the collected statistics is proceed in
 * the extension and the update of related feature space models is performed.
 * Module postprocessing.c is responsible for this part of work.
 * Also it saves query execution time and cardinality qualities of queries
 * for further analysis by AQO and DBA.
 *
 * Note that extension is transaction-dependent. That means that user has to
 * commit transaction to make model updates visible for all backends.
 *
 * More details on how cardinality estimation and models learning works.
 *
 * For each node we consider it induced feature subspace. Two nodes belongs
 * to the same feature subspace if their base relations are equal, their
 * clause sets are similar (i. e. their clauses may differ only by constant
 * values), and their classes of equivalence with size more than two are common.
 *
 * For each feature subspace we consider the selectivities of clauses which are
 * not in one of three-or-more-variables equivalence class as features of the
 * node. So each node is mapped into real-valued vector in the unit hypercube.
 * So our statistics for feature subspace is a set of such vectors with true
 * cardinalities of their corresponding nodes.
 *
 * That is how we state machine learning problem: we build the regressor from
 * each feature subspace (i. e. from clause selectivities) to cardinality.
 * More precisely, we regress vector of logarithms of clause selectivities to
 * logarithm of cardinality (that was done to set the scale which is  suitable
 * to the problem semantics and to the machine learning method). To aviod -infs
 * we lower bounded logarithms of cardinalities with 0 and logarithms of
 * selectivities with -30.
 *
 * The details of the machine learning method are available in module
 * machine_learning.c.
 *
 * Modules path_utils.c and utils.c are described by their names.
 *
 * Module hash.c computes hashes of queries and feature subspaces. The hashes
 * fulfill the properties described below.
 *
 * Module storage.c is responsible for storage query settings and models
 * (i. e. all information which is used in extension).
 *
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/aqo.h
 *
 */
#ifndef __ML_CARD_H__
#define __ML_CARD_H__

#include <math.h>

#include "access/hash.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "executor/execdesc.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "optimizer/cost.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

#include "machine_learning.h"
//#include "storage.h"

/* Check PostgreSQL version (9.6.0 contains important changes in planner) */
#if PG_VERSION_NUM < 90600
	#error "Cannot build aqo with PostgreSQL version lower than 9.6.0"
#endif

/* Strategy of determining feature space for new queries. */
typedef enum
{
	/* Creates new feature space for each query type with auto-tuning enabled */
	AQO_MODE_INTELLIGENT,
	/* Treats new query types as linked to the common feature space */
	AQO_MODE_FORCED,
	/* New query types are not linked with any feature space */
	AQO_MODE_CONTROLLED,
	/* Creates new feature space for each query type without auto-tuning */
	AQO_MODE_LEARN,
	/* Use only current AQO estimations, without learning or tuning */
	AQO_MODE_FROZEN,
	/* Aqo is disabled for all queries */
	AQO_MODE_DISABLED,
}	AQO_MODE;

extern int	aqo_mode;
extern bool	force_collect_stat;
extern bool aqo_show_hash;
extern bool aqo_show_details;
extern int aqo_join_threshold;
extern bool use_wide_search;

/* Parameters for current query */
typedef struct QueryContextData
{
	uint64		query_hash;
	uint64		fspace_hash;
	bool		learn_aqo;
	bool		use_aqo;
	bool		auto_tuning;
	bool		collect_stat;
	bool		adding_query;
	bool		explain_only;

	/*
	 * Timestamp of start of query planning process. Must be zeroed on execution
	 * start or in the case of ERROR. Query context is stored in an query env
	 * field. So, if query has a cached plan, a planning step could be skipped
	 * by an optimizer. We should realize it at an execution stage by zero value
	 * of this field.
	 */
	instr_time	start_planning_time;

	instr_time	start_execution_time;
	double		planning_time;
} QueryContextData;

struct StatEntry;

extern double predicted_ppi_rows;
extern double fss_ppi_hash;

/* Parameters of autotuning */
extern int	aqo_stat_size;
extern int	auto_tuning_window_size;
extern double auto_tuning_exploration;
extern int	auto_tuning_max_iterations;
extern int	auto_tuning_infinite_loop;
extern double auto_tuning_convergence_error;

/* Machine learning parameters */

extern int	aqo_k;
extern double log_selectivity_lower_bound;

/* Parameters for current query */
extern QueryContextData query_context;
extern int njoins;

/* AQO Memory contexts */
extern MemoryContext AQOTopMemCtx;
extern MemoryContext AQOCacheMemCtx;
extern MemoryContext AQOPredictMemCtx;
extern MemoryContext AQOLearnMemCtx;

/* Saved hook values in case of unload */
extern post_parse_analyze_hook_type prev_post_parse_analyze_hook;
extern planner_hook_type prev_planner_hook;
extern ExecutorStart_hook_type prev_ExecutorStart_hook;
extern ExecutorRun_hook_type prev_ExecutorRun;
extern ExecutorEnd_hook_type prev_ExecutorEnd_hook;
extern set_baserel_rows_estimate_hook_type
										prev_set_foreign_rows_estimate_hook;
extern set_baserel_rows_estimate_hook_type
										prev_set_baserel_rows_estimate_hook;
extern get_parameterized_baserel_size_hook_type
									prev_get_parameterized_baserel_size_hook;
extern set_joinrel_size_estimates_hook_type
										prev_set_joinrel_size_estimates_hook;
extern get_parameterized_joinrel_size_hook_type
									prev_get_parameterized_joinrel_size_hook;
extern ExplainOnePlan_hook_type prev_ExplainOnePlan_hook;
extern ExplainOneNode_hook_type prev_ExplainOneNode_hook;

extern void ppi_hook(ParamPathInfo *ppi);

/* Hash functions */
void get_eclasses(List *clauselist, int *nargs, int **args_hash,
				  int **eclass_hash);
int get_clause_hash(Expr *clause, int nargs, int *args_hash, int *eclass_hash);


/* Storage interaction */
extern bool load_fss_ext(uint64 fs, int fss, OkNNrdata *data, List **reloids,
						 bool isSafe);
extern bool update_fss_ext(uint64 fs, int fss, OkNNrdata *data,
						   List *reloids, bool isTimedOut);

/* Query preprocessing hooks */
extern void print_into_explain(PlannedStmt *plannedstmt, IntoClause *into,
							   ExplainState *es, const char *queryString,
							   ParamListInfo params,
							   const instr_time *planduration,
							   QueryEnvironment *queryEnv);
extern void print_node_explain(ExplainState *es, PlanState *ps, Plan *plan);

/* Cardinality estimation */
extern double predict_for_relation(List *restrict_clauses, List *selectivities,
								   List *relsigns, int *fss);

/* Query execution statistics collecting hooks */
void aqo_ExecutorStart(QueryDesc *queryDesc, int eflags);
void aqo_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
					 uint64 count, bool execute_once);
void aqo_ExecutorEnd(QueryDesc *queryDesc);

/* Automatic query tuning */
extern void automatical_query_tuning(uint64 query_hash, struct StatEntry *stat);

/* Utilities */
extern int int_cmp(const void *a, const void *b);
extern int double_cmp(const void *a, const void *b);
extern int *argsort(void *a, int n, size_t es,
					int (*cmp) (const void *, const void *));
extern int *inverse_permutation(int *a, int n);

/* Selectivity cache for parametrized baserels */
extern void cache_selectivity(int clause_hash, int relid, int global_relid,
							  double selectivity);
extern double *selectivity_cache_find_global_relid(int clause_hash,
												   int global_relid);
extern void selectivity_cache_clear(void);

extern bool IsQueryDisabled(void);

extern List *cur_classes;
#endif
