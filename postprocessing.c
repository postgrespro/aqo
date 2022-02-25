/*
 *******************************************************************************
 *
 *	QUERY EXECUTION STATISTICS COLLECTING UTILITIES
 *
 * The module which updates data in the feature space linked with executed query
 * type using obtained query execution statistics.
 * Works only if aqo_learn is on.
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/postprocessing.c
 *
 */

#include "postgres.h"

#include "access/parallel.h"
#include "optimizer/optimizer.h"
#include "postgres_fdw.h"
#include "utils/queryenvironment.h"

#include "aqo.h"
#include "hash.h"
#include "path_utils.h"
#include "preprocessing.h"
#include "learn_cache.h"


typedef struct
{
	List *clauselist;
	List *selectivities;
	List *relidslist;
	bool learn;
	bool isTimedOut; /* Is execution was interrupted by timeout? */
} aqo_obj_stat;

static double cardinality_sum_errors;
static int	cardinality_num_objects;

/*
 * Store an AQO-related query data into the Query Environment structure.
 *
 * It is very sad that we have to use such unsuitable field, but alternative is
 * to introduce a private field in a PlannedStmt struct.
 * It is needed to recognize stored Query-related aqo data in the query
 * environment field.
 */
static char *AQOPrivateData = "AQOPrivateData";
static char *PlanStateInfo = "PlanStateInfo";


/* Query execution statistics collecting utilities */
static void atomic_fss_learn_step(uint64 fhash, int fss_hash, int ncols,
								  double **matrix, double *targets,
								  double *features, double target,
								  List *relids, bool isTimedOut);
static bool learnOnPlanState(PlanState *p, void *context);
static void learn_agg_sample(aqo_obj_stat *ctx, List *relidslist,
							 double true_cardinality, Plan *plan,
							 bool notExecuted);
static void learn_sample(aqo_obj_stat *ctx, List *relidslist,
						 double true_cardinality, Plan *plan, bool notExecuted);
static List *restore_selectivities(List *clauselist,
								   List *relidslist,
								   JoinType join_type,
								   bool was_parametrized);
static void update_query_stat_row(double *et, int *et_size,
								  double *pt, int *pt_size,
								  double *ce, int *ce_size,
								  double planning_time,
								  double execution_time,
								  double cardinality_error,
								  int64 *n_exec);
static void StoreToQueryEnv(QueryDesc *queryDesc);
static void StorePlanInternals(QueryDesc *queryDesc);
static bool ExtractFromQueryEnv(QueryDesc *queryDesc);


/*
 * This is the critical section: only one runner is allowed to be inside this
 * function for one feature subspace.
 * matrix and targets are just preallocated memory for computations.
 */
static void
atomic_fss_learn_step(uint64 fhash, int fss_hash, int ncols,
					 double **matrix, double *targets,
					 double *features, double target,
					 List *relids, bool isTimedOut)
{
	LOCKTAG	tag;
	int		nrows;

	init_lock_tag(&tag, (uint32) fhash, fss_hash);
	LockAcquire(&tag, ExclusiveLock, false, false);

	if (!load_fss_ext(fhash, fss_hash, ncols, matrix, targets, &nrows, NULL, !isTimedOut))
		nrows = 0;

	nrows = OkNNr_learn(nrows, ncols, matrix, targets, features, target);
	update_fss_ext(fhash, fss_hash, nrows, ncols, matrix, targets, relids,
				   isTimedOut);

	LockRelease(&tag, ExclusiveLock, false);
}

static void
learn_agg_sample(aqo_obj_stat *ctx, List *relidslist,
			 double true_cardinality, Plan *plan, bool notExecuted)
{
	uint64 fhash = query_context.fspace_hash;
	int child_fss;
	int fss;
	double target;
	double	*matrix[aqo_K];
	double	targets[aqo_K];
	AQOPlanNode *aqo_node = get_aqo_plan_node(plan, false);
	int i;

	/*
	 * Learn 'not executed' nodes only once, if no one another knowledge exists
	 * for current feature subspace.
	 */
	if (notExecuted && aqo_node->prediction > 0.)
		return;

	target = log(true_cardinality);
	child_fss = get_fss_for_object(relidslist, ctx->clauselist, NIL, NULL, NULL);
	fss = get_grouped_exprs_hash(child_fss, aqo_node->grouping_exprs);

	for (i = 0; i < aqo_K; i++)
		matrix[i] = NULL;
	/* Critical section */
	atomic_fss_learn_step(fhash, fss,
						  0, matrix, targets, NULL, target,
						  relidslist, ctx->isTimedOut);
	/* End of critical section */
}

/*
 * For given object (i. e. clauselist, selectivities, relidslist, predicted and
 * true cardinalities) performs learning procedure.
 */
static void
learn_sample(aqo_obj_stat *ctx, List *relidslist,
			 double true_cardinality, Plan *plan, bool notExecuted)
{
	uint64		fhash = query_context.fspace_hash;
	int		fss_hash;
	int		nfeatures;
	double	*matrix[aqo_K];
	double	targets[aqo_K];
	double	*features;
	double	target;
	int		i;
	AQOPlanNode *aqo_node = get_aqo_plan_node(plan, false);

	target = log(true_cardinality);
	fss_hash = get_fss_for_object(relidslist, ctx->clauselist,
								  ctx->selectivities, &nfeatures, &features);

	/* Only Agg nodes can have non-empty a grouping expressions list. */
	Assert(!IsA(plan, Agg) || aqo_node->grouping_exprs != NIL);

	/*
	 * Learn 'not executed' nodes only once, if no one another knowledge exists
	 * for current feature subspace.
	 */
	if (notExecuted && aqo_node->prediction > 0)
		return;

	if (nfeatures > 0)
		for (i = 0; i < aqo_K; ++i)
			matrix[i] = palloc(sizeof(double) * nfeatures);

	/* Critical section */
	atomic_fss_learn_step(fhash, fss_hash,
						  nfeatures, matrix, targets, features, target,
						  relidslist, ctx->isTimedOut);
	/* End of critical section */

	if (nfeatures > 0)
		for (i = 0; i < aqo_K; ++i)
			pfree(matrix[i]);

	pfree(features);
}

/*
 * For given node specified by clauselist, relidslist and join_type restores
 * the same selectivities of clauses as were used at query optimization stage.
 */
List *
restore_selectivities(List *clauselist,
					  List *relidslist,
					  JoinType join_type,
					  bool was_parametrized)
{
	List		*lst = NIL;
	ListCell	*l;
	int			i = 0;
	bool		parametrized_sel;
	int			nargs;
	int			*args_hash;
	int			*eclass_hash;
	double		*cur_sel;
	int			cur_hash;
	int			cur_relid;

	parametrized_sel = was_parametrized && (list_length(relidslist) == 1);
	if (parametrized_sel)
	{
		cur_relid = linitial_int(relidslist);
		get_eclasses(clauselist, &nargs, &args_hash, &eclass_hash);
	}

	foreach(l, clauselist)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		cur_sel = NULL;
		if (parametrized_sel)
		{
			cur_hash = get_clause_hash(rinfo->clause, nargs,
									   args_hash, eclass_hash);
			cur_sel = selectivity_cache_find_global_relid(cur_hash, cur_relid);
			if (cur_sel == NULL)
			{
				if (join_type == JOIN_INNER)
					cur_sel = &rinfo->norm_selec;
				else
					cur_sel = &rinfo->outer_selec;
			}
		}
		else if (join_type == JOIN_INNER)
			cur_sel = &rinfo->norm_selec;
		else
			cur_sel = &rinfo->outer_selec;

		lst = lappend(lst, cur_sel);
		i++;
	}

	if (parametrized_sel)
	{
		pfree(args_hash);
		pfree(eclass_hash);
	}

	return lst;
}

static bool
IsParallelTuplesProcessing(const Plan *plan, bool IsParallel)
{
	if (IsParallel && (plan->parallel_aware || nodeTag(plan) == T_HashJoin ||
		nodeTag(plan) == T_MergeJoin || nodeTag(plan) == T_NestLoop))
		return true;
	return false;
}

/*
 * learn_subplan_recurse
 *
 * Emphasize recursion operation into separate function because of increasing
 * complexity of this logic.
 */
static bool
learn_subplan_recurse(PlanState *p, aqo_obj_stat *ctx)
{
	List *saved_subplan_list = NIL;
	List *saved_initplan_list = NIL;
	ListCell *lc;

	if (!p->instrument)
		return true;

	if (!ctx->isTimedOut)
		InstrEndLoop(p->instrument);
	else if (p->instrument->running)
	{
		/*
		 * We can't use node instrumentation functions because after the end
		 * of this timeout handler query can work for some time.
		 * We change ntuples and nloops to unify walking logic and because we
		 * know that the query execution results meaningless.
		 */
		p->instrument->ntuples += p->instrument->tuplecount;
		p->instrument->nloops += 1;

		/*
		 * TODO: can we simply use ExecParallelCleanup to implement gathering of
		 * instrument data in the case of parallel workers?
		 */
	}

	saved_subplan_list = p->subPlan;
	saved_initplan_list = p->initPlan;
	p->subPlan = NIL;
	p->initPlan = NIL;

	if (planstate_tree_walker(p, learnOnPlanState, (void *) ctx))
		return true;

	/*
	 * Learn on subplans and initplans separately. Discard learn context of these
	 * subplans because we will use their fss'es directly.
	 */
	foreach(lc, saved_subplan_list)
	{
		SubPlanState *sps = lfirst_node(SubPlanState, lc);
		aqo_obj_stat SPCtx = {NIL, NIL, NIL, ctx->learn, ctx->isTimedOut};

		if (learnOnPlanState(sps->planstate, (void *) &SPCtx))
			return true;
	}
	foreach(lc, saved_initplan_list)
	{
		SubPlanState *sps = lfirst_node(SubPlanState, lc);
		aqo_obj_stat SPCtx = {NIL, NIL, NIL, ctx->learn, ctx->isTimedOut};

		if (learnOnPlanState(sps->planstate, (void *) &SPCtx))
			return true;
	}

	p->subPlan = saved_subplan_list;
	p->initPlan = saved_initplan_list;
	return false;
}

static bool
should_learn(aqo_obj_stat *ctx, double predicted, double *nrows)
{
	if (ctx->isTimedOut)
	{
		if (ctx->learn && *nrows > predicted * 1.2)
		{
			*nrows += (*nrows - predicted) * 10.;
			return true;
		}
	}
	else if (ctx->learn)
		return true;

	return false;
}

/*
 * Walks over obtained PlanState tree, collects relation objects with their
 * clauses, selectivities and relids and passes each object to learn_sample.
 *
 * Returns clauselist, selectivities and relids.
 * Store observed subPlans into other_plans list.
 *
 * We use list_copy() of AQOPlanNode->clauses and AQOPlanNode->relids
 * because the plan may be stored in the cache after this. Operation
 * list_concat() changes input lists and may destruct cached plan.
 */
static bool
learnOnPlanState(PlanState *p, void *context)
{
	aqo_obj_stat *ctx = (aqo_obj_stat *) context;
	aqo_obj_stat SubplanCtx = {NIL, NIL, NIL, ctx->learn, ctx->isTimedOut};
	double predicted = 0.;
	double learn_rows = 0.;
	AQOPlanNode *aqo_node;
	bool notExecuted = false;

	/* Recurse into subtree and collect clauses. */
	if (learn_subplan_recurse(p, &SubplanCtx))
		/* If something goes wrong, return quickly. */
		return true;

	aqo_node = get_aqo_plan_node(p->plan, false);

	/*
	 * Compute real value of rows, passed through this node. Summarize rows
	 * for parallel workers.
	 * If 'never executed' node will be found - set specific sign, because we
	 * allow to learn on such node only once.
	 */
	if (p->instrument->nloops > 0.)
	{
		/* If we can strongly calculate produced rows, do it. */
		if (p->worker_instrument &&
			IsParallelTuplesProcessing(p->plan, aqo_node->parallel_divisor > 0))
		{
			double wnloops = 0.;
			double wntuples = 0.;
			int i;

			for (i = 0; i < p->worker_instrument->num_workers; i++)
			{
				double t = p->worker_instrument->instrument[i].ntuples;
				double l = p->worker_instrument->instrument[i].nloops;

				if (l <= 0)
					continue;

				wntuples += t;
				wnloops += l;
				learn_rows += t/l;
			}

			Assert(p->instrument->nloops >= wnloops);
			Assert(p->instrument->ntuples >= wntuples);
			if (p->instrument->nloops - wnloops > 0.5)
				learn_rows += (p->instrument->ntuples - wntuples) /
								(p->instrument->nloops - wnloops);
		}
		else
			/* This node does not required to sum tuples of each worker
			 * to calculate produced rows. */
			learn_rows = p->instrument->ntuples / p->instrument->nloops;
	}
	else
	{
		/* The case of 'not executed' node. */
		learn_rows = 1.;
		notExecuted = true;
	}

	/*
	 * Calculate predicted cardinality.
	 * We could find a positive value of predicted cardinality in the case of
	 * reusing plan caused by the rewriting procedure.
	 * Also it may be caused by using of a generic plan.
	 */
	if (aqo_node->prediction > 0. && query_context.use_aqo)
	{
		/* AQO made prediction. use it. */
		predicted = aqo_node->prediction;
	}
	else if (IsParallelTuplesProcessing(p->plan, aqo_node->parallel_divisor > 0))
		/*
		 * AQO didn't make a prediction and we need to calculate real number
		 * of tuples passed because of parallel workers.
		 */
		predicted = p->plan->plan_rows * aqo_node->parallel_divisor;
	else
		/* No AQO prediction. Parallel workers not used for this plan node. */
		predicted = p->plan->plan_rows;

	if (!ctx->learn && query_context.collect_stat)
	{
		double p,l;

		/* Special case of forced gathering of statistics. */
		Assert(predicted >= 0 && learn_rows >= 0);
		p = (predicted < 1) ? 0 : log(predicted);
		l = (learn_rows < 1) ? 0 : log(learn_rows);
		cardinality_sum_errors += fabs(p - l);
		cardinality_num_objects += 1;
		return false;
	}
	else if (!ctx->learn)
		return true;

	/*
	 * Need learn.
	 */

	/*
	 * It is needed for correct exp(result) calculation.
	 * Do it before cardinality error estimation because we can predict no less
	 * than 1 tuple, but get zero tuples.
	 */
	predicted = clamp_row_est(predicted);
	learn_rows = clamp_row_est(learn_rows);

	/* Exclude "not executed" nodes from error calculation to reduce fluctuations. */
	if (!notExecuted)
	{
		cardinality_sum_errors += fabs(predicted - learn_rows);
		cardinality_num_objects += 1;
	}

	/*
	 * Some nodes inserts after planning step (See T_Hash node type).
	 * In this case we have'nt AQO prediction and fss record.
	 */
	if (aqo_node->had_path)
	{
		List *cur_selectivities;

		cur_selectivities = restore_selectivities(aqo_node->clauses,
												  aqo_node->relids,
												  aqo_node->jointype,
												  aqo_node->was_parametrized);
		SubplanCtx.selectivities = list_concat(SubplanCtx.selectivities,
															cur_selectivities);
		SubplanCtx.clauselist = list_concat(SubplanCtx.clauselist,
											list_copy(aqo_node->clauses));

		if (aqo_node->relids != NIL)
		{
			/*
			 * This plan can be stored as a cached plan. In the case we will have
			 * bogus path_relids field (changed by list_concat routine) at the
			 * next usage (and aqo-learn) of this plan.
			 */
			ctx->relidslist = list_copy(aqo_node->relids);

			if (p->instrument)
			{
				Assert(predicted >= 1. && learn_rows >= 1.);

				if (should_learn(ctx, predicted, &learn_rows))
				{
					if (ctx->isTimedOut && aqo_show_details)
						elog(NOTICE, "[AQO] Learn on partially executed plan node. fs: %lu, fss: %d, predicted rows: %.0lf, updated prediction: %.0lf",
							 query_context.query_hash, aqo_node->fss, predicted, learn_rows);

					if (IsA(p, AggState))
						learn_agg_sample(&SubplanCtx,
										 aqo_node->relids, learn_rows,
										 p->plan, notExecuted);

					else
						learn_sample(&SubplanCtx,
									 aqo_node->relids, learn_rows,
									 p->plan, notExecuted);

					if (!ctx->isTimedOut)
						lc_remove_fss(query_context.query_hash, aqo_node->fss);
				}
			}
		}
	}

	ctx->clauselist = list_concat(ctx->clauselist, SubplanCtx.clauselist);
	ctx->selectivities = list_concat(ctx->selectivities,
													SubplanCtx.selectivities);
	return false;
}

/*
 * Updates given row of query statistics:
 * et - execution time
 * pt - planning time
 * ce - cardinality error
 */
void
update_query_stat_row(double *et, int *et_size,
					  double *pt, int *pt_size,
					  double *ce, int *ce_size,
					  double planning_time,
					  double execution_time,
					  double cardinality_error,
					  int64 *n_exec)
{
	int i;

	/*
	 * If plan contains one or more "never visited" nodes, cardinality_error
	 * have -1 value and will be written to the knowledge base. User can use it
	 * as a sign that AQO ignores this query.
	 */
	if (*ce_size >= aqo_stat_size)
			for (i = 1; i < aqo_stat_size; ++i)
				ce[i - 1] = ce[i];
		*ce_size = (*ce_size >= aqo_stat_size) ? aqo_stat_size : (*ce_size + 1);
		ce[*ce_size - 1] = cardinality_error;

	if (*et_size >= aqo_stat_size)
		for (i = 1; i < aqo_stat_size; ++i)
			et[i - 1] = et[i];

	*et_size = (*et_size >= aqo_stat_size) ? aqo_stat_size : (*et_size + 1);
	et[*et_size - 1] = execution_time;

	if (*pt_size >= aqo_stat_size)
		for (i = 1; i < aqo_stat_size; ++i)
			pt[i - 1] = pt[i];

	*pt_size = (*pt_size >= aqo_stat_size) ? aqo_stat_size : (*pt_size + 1);
	pt[*pt_size - 1] = planning_time; /* Just remember: planning time can be negative. */
	(*n_exec)++;
}

/*****************************************************************************
 *
 *	QUERY EXECUTION STATISTICS COLLECTING HOOKS
 *
 *****************************************************************************/

/*
 * Set up flags to store cardinality statistics.
 */
void
aqo_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	instr_time now;
	bool use_aqo;

	/*
	 * If the plan pulled from a plan cache, planning don't needed. Restore
	 * query context from the query environment.
	 */
	if (ExtractFromQueryEnv(queryDesc))
		Assert(INSTR_TIME_IS_ZERO(query_context.start_planning_time));

	use_aqo = !IsQueryDisabled() && !IsParallelWorker() &&
				(query_context.use_aqo || query_context.learn_aqo ||
				force_collect_stat);

	if (use_aqo)
	{
		if (!INSTR_TIME_IS_ZERO(query_context.start_planning_time))
		{
			INSTR_TIME_SET_CURRENT(now);
			INSTR_TIME_SUBTRACT(now, query_context.start_planning_time);
			query_context.planning_time = INSTR_TIME_GET_DOUBLE(now);
		}
		else
			/*
			 * Should set anyway. It will be stored in a query env. The query
			 * can be reused later by extracting from a plan cache.
			 */
			query_context.planning_time = -1;

		/*
		 * To zero this timestamp preventing a false time calculation in the
		 * case, when the plan was got from a plan cache.
		 */
		INSTR_TIME_SET_ZERO(query_context.start_planning_time);

		/* Make a timestamp for execution stage. */
		INSTR_TIME_SET_CURRENT(now);
		query_context.start_execution_time = now;

		query_context.explain_only = ((eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0);

		if ((query_context.learn_aqo || force_collect_stat) &&
			!query_context.explain_only)
			queryDesc->instrument_options |= INSTRUMENT_ROWS;

		/* Save all query-related parameters into the query context. */
		StoreToQueryEnv(queryDesc);
	}

	if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (use_aqo)
		StorePlanInternals(queryDesc);
}

#include "utils/timeout.h"

static struct
{
	TimeoutId id;
	QueryDesc *queryDesc;
} timeoutCtl = {0, NULL};

static int exec_nested_level = 0;

static void
aqo_timeout_handler(void)
{
	aqo_obj_stat ctx = {NIL, NIL, NIL, false, false};

	if (!timeoutCtl.queryDesc || !ExtractFromQueryEnv(timeoutCtl.queryDesc))
		return;

	/* Now we can analyze execution state of the query. */

	ctx.learn = query_context.learn_aqo;
	ctx.isTimedOut = true;

	elog(NOTICE, "[AQO] Time limit for execution of the statement was expired. Try to learn on partial data.");
	learnOnPlanState(timeoutCtl.queryDesc->planstate, (void *) &ctx);
}

static bool
set_timeout_if_need(QueryDesc *queryDesc)
{
	TimestampTz	fin_time;

	if (!get_timeout_active(STATEMENT_TIMEOUT) || !aqo_learn_statement_timeout)
		return false;

	if (!ExtractFromQueryEnv(queryDesc))
		return false;

	if (IsQueryDisabled() || IsParallelWorker() ||
		!(query_context.use_aqo || query_context.learn_aqo))
		return false;

	/*
	 * Statement timeout exists. AQO should create user timeout right before the
	 * timeout.
	 */

	if (timeoutCtl.id < USER_TIMEOUT)
		/* Register once per backend, because of timeouts implementation. */
		timeoutCtl.id = RegisterTimeout(USER_TIMEOUT, aqo_timeout_handler);
	else
		Assert(!get_timeout_active(timeoutCtl.id));

	fin_time = get_timeout_finish_time(STATEMENT_TIMEOUT);
	enable_timeout_at(timeoutCtl.id, fin_time - 1);

	/* Save pointer to queryDesc to use at learning after a timeout interruption. */
	timeoutCtl.queryDesc = queryDesc;
	return true;
}

/*
 * ExecutorRun hook.
 */
void
aqo_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
				 bool execute_once)
{
	bool		timeout_enabled = false;

	if (exec_nested_level <= 0)
		timeout_enabled = set_timeout_if_need(queryDesc);

	Assert(!timeout_enabled ||
		   (timeoutCtl.queryDesc && timeoutCtl.id >= USER_TIMEOUT));

	exec_nested_level++;

	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_FINALLY();
	{
		exec_nested_level--;
		timeoutCtl.queryDesc = NULL;

		if (timeout_enabled)
			disable_timeout(timeoutCtl.id, false);
	}
	PG_END_TRY();
}

/*
 * General hook which runs before ExecutorEnd and collects query execution
 * cardinality statistics.
 * Also it updates query execution statistics in aqo_query_stat.
 */
void
aqo_ExecutorEnd(QueryDesc *queryDesc)
{
	double execution_time;
	double cardinality_error;
	QueryStat *stat = NULL;
	instr_time endtime;
	EphemeralNamedRelation enr = get_ENR(queryDesc->queryEnv, PlanStateInfo);
	LOCKTAG tag;

	cardinality_sum_errors = 0.;
	cardinality_num_objects = 0;

	if (!ExtractFromQueryEnv(queryDesc))
		/* AQO keep all query-related preferences at the query context.
		 * It is needed to prevent from possible recursive changes, at
		 * preprocessing stage of subqueries.
		 * If context not exist we assume AQO was disabled at preprocessing
		 * stage for this query.
		 */
		goto end;

	njoins = (enr != NULL) ? *(int *) enr->reldata : -1;

	Assert(!IsQueryDisabled());
	Assert(!IsParallelWorker());

	if (query_context.explain_only)
	{
		query_context.learn_aqo = false;
		query_context.collect_stat = false;
	}

	if (query_context.learn_aqo ||
		(!query_context.learn_aqo && query_context.collect_stat))
	{
		aqo_obj_stat ctx = {NIL, NIL, NIL, query_context.learn_aqo, false};

		/*
		 * Analyze plan if AQO need to learn or need to collect statistics only.
		 */
		learnOnPlanState(queryDesc->planstate, (void *) &ctx);
		list_free(ctx.clauselist);
		list_free(ctx.relidslist);
		list_free(ctx.selectivities);
	}

	if (query_context.collect_stat)
		stat = get_aqo_stat(query_context.query_hash);

	{
		/* Calculate execution time. */
		INSTR_TIME_SET_CURRENT(endtime);
		INSTR_TIME_SUBTRACT(endtime, query_context.start_execution_time);
		execution_time = INSTR_TIME_GET_DOUBLE(endtime);

		if (cardinality_num_objects > 0)
			cardinality_error = cardinality_sum_errors / cardinality_num_objects;
		else
			cardinality_error = -1;
		Assert(query_context.query_hash>=0);
		/* Prevent concurrent updates. */
		init_lock_tag(&tag, (uint32) query_context.query_hash,//my code
					 (uint32) query_context.fspace_hash);//possible here
		LockAcquire(&tag, ExclusiveLock, false, false);

		if (stat != NULL)
		{
			/* Calculate AQO statistics. */
			if (query_context.use_aqo)
				/* For the case, when query executed with AQO predictions. */
				update_query_stat_row(stat->execution_time_with_aqo,
									 &stat->execution_time_with_aqo_size,
									 stat->planning_time_with_aqo,
									 &stat->planning_time_with_aqo_size,
									 stat->cardinality_error_with_aqo,
									 &stat->cardinality_error_with_aqo_size,
									 query_context.planning_time,
									 execution_time,
									 cardinality_error,
									 &stat->executions_with_aqo);
			else
				/* For the case, when query executed without AQO predictions. */
				update_query_stat_row(stat->execution_time_without_aqo,
									 &stat->execution_time_without_aqo_size,
									 stat->planning_time_without_aqo,
									 &stat->planning_time_without_aqo_size,
									 stat->cardinality_error_without_aqo,
									 &stat->cardinality_error_without_aqo_size,
									 query_context.planning_time,
									 execution_time,
									 cardinality_error,
									 &stat->executions_without_aqo);

			/* Store all learn data into the AQO service relations. */
			Assert(query_context.query_hash>=0);
			if (!query_context.adding_query && query_context.auto_tuning)
				automatical_query_tuning(query_context.query_hash, stat);

			/* Write AQO statistics to the aqo_query_stat table */
			update_aqo_stat(query_context.fspace_hash, stat);
			pfree_query_stat(stat);
		}

		/* Allow concurrent queries to update this feature space. */
		LockRelease(&tag, ExclusiveLock, false);
	}

	selectivity_cache_clear();
	cur_classes = ldelete_uint64(cur_classes, query_context.query_hash);

end:
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	/*
	 * standard_ExecutorEnd clears the queryDesc->planstate. After this point no
	 * one operation with the plan can be made.
	 */

	timeoutCtl.queryDesc = NULL;
}

/*
 * Store into a query environment field an AQO data related to the query.
 * We introduce this machinery to avoid problems with subqueries, induced by
 * top-level query.
 * If such enr exists, routine will replace it with current value of the
 * query context.
 */
static void
StoreToQueryEnv(QueryDesc *queryDesc)
{
	EphemeralNamedRelation	enr;
	int	qcsize = sizeof(QueryContextData);
	MemoryContext	oldCxt;
	bool newentry = false;

	oldCxt = MemoryContextSwitchTo(GetMemoryChunkContext(queryDesc->plannedstmt));

	if (queryDesc->queryEnv == NULL)
			queryDesc->queryEnv = create_queryEnv();

	enr = get_ENR(queryDesc->queryEnv, AQOPrivateData);
	if (enr == NULL)
	{
		/* If such query environment don't exists, allocate new. */
		enr = palloc0(sizeof(EphemeralNamedRelationData));
		newentry = true;
	}

	enr->md.name = AQOPrivateData;
	enr->md.enrtuples = 0;
	enr->md.enrtype = 0;
	enr->md.reliddesc = InvalidOid;
	enr->md.tupdesc = NULL;
	enr->reldata = palloc0(qcsize);
	memcpy(enr->reldata, &query_context, qcsize);

	if (newentry)
		register_ENR(queryDesc->queryEnv, enr);

	MemoryContextSwitchTo(oldCxt);
}

static bool
calculateJoinNum(PlanState *ps, void *context)
{
	int *njoins_ptr = (int *) context;

	planstate_tree_walker(ps, calculateJoinNum, context);

	if (nodeTag(ps->plan) == T_NestLoop ||
		nodeTag(ps->plan) == T_MergeJoin ||
		nodeTag(ps->plan) == T_HashJoin)
		(*njoins_ptr)++;

	return false;
}

static void
StorePlanInternals(QueryDesc *queryDesc)
{
	EphemeralNamedRelation enr;
	MemoryContext	oldCxt;
	bool newentry = false;

	njoins = 0;
	planstate_tree_walker(queryDesc->planstate, calculateJoinNum, &njoins);

	oldCxt = MemoryContextSwitchTo(GetMemoryChunkContext(queryDesc->plannedstmt));

	if (queryDesc->queryEnv == NULL)
			queryDesc->queryEnv = create_queryEnv();

	enr = get_ENR(queryDesc->queryEnv, PlanStateInfo);
	if (enr == NULL)
	{
		/* If such query environment field doesn't exist, allocate new. */
		enr = palloc0(sizeof(EphemeralNamedRelationData));
		newentry = true;
	}

	enr->md.name = PlanStateInfo;
	enr->md.enrtuples = 0;
	enr->md.enrtype = 0;
	enr->md.reliddesc = InvalidOid;
	enr->md.tupdesc = NULL;
	enr->reldata = palloc0(sizeof(int));
	memcpy(enr->reldata, &njoins, sizeof(int));

	if (newentry)
		register_ENR(queryDesc->queryEnv, enr);

	MemoryContextSwitchTo(oldCxt);
}

/*
 * Restore AQO data, related to the query.
 */
static bool
ExtractFromQueryEnv(QueryDesc *queryDesc)
{
	EphemeralNamedRelation	enr;

	/* This is a very rare case when we don't load aqo as shared library during
	 * startup perform 'CREATE EXTENSION aqo' command in the backend and first
	 * query in any another backend is 'UPDATE aqo_queries...'. In this case
	 * ExecutorEnd hook will be executed without ExecutorStart hook.
	 */
	if (queryDesc->queryEnv == NULL)
		return false;

	enr = get_ENR(queryDesc->queryEnv, AQOPrivateData);

	if (enr == NULL)
		return false;

	memcpy(&query_context, enr->reldata, sizeof(QueryContextData));

	return true;
}

void
print_node_explain(ExplainState *es, PlanState *ps, Plan *plan)
{
	int				wrkrs = 1;
	double			error = -1.;
	AQOPlanNode	   *aqo_node;

	/* Extension, which took a hook early can be executed early too. */
	if (prev_ExplainOneNode_hook)
		prev_ExplainOneNode_hook(es, ps, plan);

	if (IsQueryDisabled() || !plan || es->format != EXPLAIN_FORMAT_TEXT)
		return;

	aqo_node = get_aqo_plan_node(plan, false);

	if (!aqo_show_details || !ps)
		goto explain_end;

	if (!ps->instrument)
		/* We can show only prediction, without error calculation */
		goto explain_print;

	if (ps->worker_instrument &&
		IsParallelTuplesProcessing(plan, aqo_node->parallel_divisor > 0))
	{
		int i;

		for (i = 0; i < ps->worker_instrument->num_workers; i++)
		{
			Instrumentation *instrument = &ps->worker_instrument->instrument[i];

			if (instrument->nloops <= 0)
				continue;

			wrkrs++;
		}
	}

explain_print:
	appendStringInfoChar(es->str, '\n');
	if (es->str->len == 0 || es->str->data[es->str->len - 1] == '\n')
		appendStringInfoSpaces(es->str, es->indent * 2);

	if (aqo_node->prediction > 0.)
	{
		appendStringInfo(es->str, "AQO: rows=%.0lf", aqo_node->prediction);

		if (ps->instrument && ps->instrument->nloops > 0.)
		{
			double rows = ps->instrument->ntuples / ps->instrument->nloops;

			error = 100. * (aqo_node->prediction - (rows*wrkrs))
									/ aqo_node->prediction;
			appendStringInfo(es->str, ", error=%.0lf%%", error);
		}
	}
	else
		appendStringInfo(es->str, "AQO not used");

explain_end:
	/* XXX: Do we really have situations when the plan is a NULL pointer? */
	if (plan && aqo_show_hash)
		appendStringInfo(es->str, ", fss=%d", aqo_node->fss);
}

/*
 * Prints if the plan was constructed with AQO.
 */
void
print_into_explain(PlannedStmt *plannedstmt, IntoClause *into,
				   ExplainState *es, const char *queryString,
				   ParamListInfo params, const instr_time *planduration,
				   QueryEnvironment *queryEnv)
{
	if (prev_ExplainOnePlan_hook)
		prev_ExplainOnePlan_hook(plannedstmt, into, es, queryString,
								 params, planduration, queryEnv);

	if (IsQueryDisabled() || !aqo_show_details)
		return;

	/* Report to user about aqo state only in verbose mode */
	ExplainPropertyBool("Using aqo", query_context.use_aqo, es);

	switch (aqo_mode)
	{
	case AQO_MODE_INTELLIGENT:
		ExplainPropertyText("AQO mode", "INTELLIGENT", es);
		break;
	case AQO_MODE_FORCED:
		ExplainPropertyText("AQO mode", "FORCED", es);
		break;
	case AQO_MODE_CONTROLLED:
		ExplainPropertyText("AQO mode", "CONTROLLED", es);
		break;
	case AQO_MODE_LEARN:
		ExplainPropertyText("AQO mode", "LEARN", es);
		break;
	case AQO_MODE_FROZEN:
		ExplainPropertyText("AQO mode", "FROZEN", es);
		break;
	case AQO_MODE_DISABLED:
		ExplainPropertyText("AQO mode", "DISABLED", es);
		break;
	default:
		elog(ERROR, "Bad AQO state");
		break;
	}

	/*
	 * Query class provides an user the conveniently use of the AQO
	 * auxiliary functions.
	 */
	if (aqo_mode != AQO_MODE_DISABLED || force_collect_stat)
	{
		Assert(query_context.query_hash>=0);
		if (aqo_show_hash)
			ExplainPropertyInteger("Query hash", NULL,
									query_context.query_hash, es);
		ExplainPropertyInteger("JOINS", NULL, njoins, es);
	}
}
