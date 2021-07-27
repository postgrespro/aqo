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
 * Copyright (c) 2016-2021, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/postprocessing.c
 *
 */

#include "aqo.h"
#include "hash.h"
#include "ignorance.h"
#include "path_utils.h"

#include "access/parallel.h"
#include "optimizer/optimizer.h"
#include "postgres_fdw.h"
#include "utils/queryenvironment.h"


typedef struct
{
	List *clauselist;
	List *selectivities;
	List *relidslist;
	bool learn;
} aqo_obj_stat;

static double cardinality_sum_errors;
static int	cardinality_num_objects;

/* It is needed to recognize stored Query-related aqo data in the query
 * environment field.
 */
static char *AQOPrivateData = "AQOPrivateData";
static char *PlanStateInfo = "PlanStateInfo";


/* Query execution statistics collecting utilities */
static void atomic_fss_learn_step(int fhash, int fss_hash, int ncols,
								  double **matrix, double *targets,
								  double *features, double target,
								  List *relids);
static bool learnOnPlanState(PlanState *p, void *context);
static void learn_sample(List *clauselist,
						 List *selectivities,
						 List *relidslist,
						 double true_cardinality,
						 Plan *plan,
						 bool notExecuted);
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
static void RemoveFromQueryEnv(QueryDesc *queryDesc);


/*
 * This is the critical section: only one runner is allowed to be inside this
 * function for one feature subspace.
 * matrix and targets are just preallocated memory for computations.
 */
static void
atomic_fss_learn_step(int fhash, int fss_hash, int ncols,
					 double **matrix, double *targets,
					 double *features, double target,
					 List *relids)
{
	LOCKTAG	tag;
	int		nrows;

	init_lock_tag(&tag, (uint32) fhash, (uint32) fss_hash);
	LockAcquire(&tag, ExclusiveLock, false, false);

	if (!load_fss(fhash, fss_hash, ncols, matrix, targets, &nrows, NULL))
		nrows = 0;

	nrows = OkNNr_learn(nrows, ncols, matrix, targets, features, target);
	update_fss(fhash, fss_hash, nrows, ncols, matrix, targets, relids);

	LockRelease(&tag, ExclusiveLock, false);
}

static void
learn_agg_sample(List *clauselist, List *selectivities, List *relidslist,
			 double true_cardinality, Plan *plan, bool notExecuted)
{
	int fhash = query_context.fspace_hash;
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
	if (notExecuted && aqo_node->prediction > 0)
		return;

	target = log(true_cardinality);
	child_fss = get_fss_for_object(relidslist, clauselist, NIL, NULL, NULL);
	fss = get_grouped_exprs_hash(child_fss, aqo_node->grouping_exprs);

	for (i = 0; i < aqo_K; i++)
		matrix[i] = NULL;
	/* Critical section */
	atomic_fss_learn_step(fhash, fss,
						  0, matrix, targets, NULL, target,
						  relidslist);
	/* End of critical section */
}

/*
 * For given object (i. e. clauselist, selectivities, relidslist, predicted and
 * true cardinalities) performs learning procedure.
 */
static void
learn_sample(List *clauselist, List *selectivities, List *relidslist,
			 double true_cardinality, Plan *plan, bool notExecuted)
{
	int		fhash = query_context.fspace_hash;
	int		fss_hash;
	int		nfeatures;
	double	*matrix[aqo_K];
	double	targets[aqo_K];
	double	*features;
	double	target;
	int		i;
	AQOPlanNode *aqo_node = get_aqo_plan_node(plan, false);

	target = log(true_cardinality);
	fss_hash = get_fss_for_object(relidslist, clauselist,
								  selectivities, &nfeatures, &features);

	/* Only Agg nodes can have non-empty a grouping expressions list. */
	Assert(!IsA(plan, Agg) || aqo_node->grouping_exprs != NIL);

	/*
	 * Learn 'not executed' nodes only once, if no one another knowledge exists
	 * for current feature subspace.
	 */
	if (notExecuted && aqo_node->prediction > 0)
		return;

	if (aqo_log_ignorance && aqo_node->prediction <= 0 &&
		load_fss(fhash, fss_hash, 0, NULL, NULL, NULL, NULL) )
	{
		/*
		 * If ignorance logging is enabled and the feature space was existed in
		 * the ML knowledge base, log this issue.
		 */
		update_ignorance(query_context.query_hash, fhash, fss_hash, plan);
	}

	if (nfeatures > 0)
		for (i = 0; i < aqo_K; ++i)
			matrix[i] = palloc(sizeof(double) * nfeatures);

	/* Critical section */
	atomic_fss_learn_step(fhash, fss_hash,
						  nfeatures, matrix, targets, features, target,
						  relidslist);
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
 * Emphasise recursion operation into separate function because of increasing
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
	InstrEndLoop(p->instrument);

	saved_subplan_list = p->subPlan;
	saved_initplan_list = p->initPlan;
	p->subPlan = NIL;
	p->initPlan = NIL;

	if (planstate_tree_walker(p, learnOnPlanState, (void *) ctx))
		return true;

	foreach(lc, saved_subplan_list)
	{
		SubPlanState *sps = lfirst_node(SubPlanState, lc);
		aqo_obj_stat SPCtx = {NIL, NIL, NIL, ctx->learn};

		if (learnOnPlanState(sps->planstate, (void *) &SPCtx))
			return true;
	}

	foreach(lc, saved_initplan_list)
	{
		SubPlanState *sps = lfirst_node(SubPlanState, lc);
		aqo_obj_stat SPCtx = {NIL, NIL, NIL, ctx->learn};

		if (learnOnPlanState(sps->planstate, (void *) &SPCtx))
			return true;
	}

	p->subPlan = saved_subplan_list;
	p->initPlan = saved_initplan_list;
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
	aqo_obj_stat SubplanCtx = {NIL, NIL, NIL, ctx->learn};
	double predicted = 0.;
	double learn_rows = 0.;
	AQOPlanNode *aqo_node;
	bool notExecuted = false;

	/* Recurse into subtree and collect clauses. */
	if (learn_subplan_recurse(p, &SubplanCtx))
		/* If something goes wrong, return quckly. */
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

	/* It is needed for correct exp(result) calculation. */
	predicted = clamp_row_est(predicted);
	learn_rows = clamp_row_est(learn_rows);

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
			/*
			 * This plan can be stored as cached plan. In the case we will have
			 * bogus path_relids field (changed by list_concat routine) at the
			 * next usage (and aqo-learn) of this plan.
			 */
			ctx->relidslist = list_copy(aqo_node->relids);

		if (p->instrument)
		{
			Assert(predicted >= 1. && learn_rows >= 1.);

			if (ctx->learn)
			{
				if (IsA(p, AggState))
					learn_agg_sample(SubplanCtx.clauselist, NULL,
							 		 aqo_node->relids, learn_rows,
									 p->plan, notExecuted);

				else
					learn_sample(SubplanCtx.clauselist, SubplanCtx.selectivities,
							 aqo_node->relids, learn_rows, p->plan, notExecuted);
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
	pt[*pt_size - 1] = planning_time;
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
	instr_time	current_time;
	bool use_aqo;

	use_aqo = !IsParallelWorker() && (query_context.use_aqo ||
									  query_context.learn_aqo ||
									  force_collect_stat);

	if (use_aqo)
	{
		INSTR_TIME_SET_CURRENT(current_time);
		INSTR_TIME_SUBTRACT(current_time, query_context.query_starttime);
		query_context.query_planning_time = INSTR_TIME_GET_DOUBLE(current_time);

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

	/* Plan state has initialized */
	if (use_aqo)
		StorePlanInternals(queryDesc);
}

/*
 * General hook which runs before ExecutorEnd and collects query execution
 * cardinality statistics.
 * Also it updates query execution statistics in aqo_query_stat.
 */
void
aqo_ExecutorEnd(QueryDesc *queryDesc)
{
	double totaltime;
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

	Assert(!IsParallelWorker());

	if (query_context.explain_only)
	{
		query_context.learn_aqo = false;
		query_context.collect_stat = false;
	}

	if (query_context.learn_aqo ||
		(!query_context.learn_aqo && query_context.collect_stat))
	{
		aqo_obj_stat ctx = {NIL, NIL, NIL, query_context.learn_aqo};

		/*
		 * Analyze plan if AQO need to learn or need to collect statistics only.
		 */
		learnOnPlanState(queryDesc->planstate, (void *) &ctx);
		list_free(ctx.clauselist);
		list_free(ctx.relidslist);
		list_free(ctx.selectivities);
	}

	/* Prevent concurrent updates. */
	init_lock_tag(&tag, (uint32) query_context.query_hash,
				 (uint32) query_context.fspace_hash);
	LockAcquire(&tag, ExclusiveLock, false, false);

	if (query_context.collect_stat)
	{
		INSTR_TIME_SET_CURRENT(endtime);
		INSTR_TIME_SUBTRACT(endtime, query_context.query_starttime);
		totaltime = INSTR_TIME_GET_DOUBLE(endtime);
		if (cardinality_num_objects > 0)
			cardinality_error = cardinality_sum_errors / cardinality_num_objects;
		else
			cardinality_error = -1;

		stat = get_aqo_stat(query_context.query_hash);

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
									 query_context.query_planning_time,
									 totaltime - query_context.query_planning_time,
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
									 query_context.query_planning_time,
									 totaltime - query_context.query_planning_time,
									 cardinality_error,
									 &stat->executions_without_aqo);
		}
	}
	selectivity_cache_clear();

	/*
	 * Store all learn data into the AQO service relations.
	 */
	if ((query_context.collect_stat) && (stat != NULL))
	{
		if (!query_context.adding_query && query_context.auto_tuning)
			automatical_query_tuning(query_context.query_hash, stat);

		/* Write AQO statistics to the aqo_query_stat table */
		update_aqo_stat(query_context.fspace_hash, stat);
		pfree_query_stat(stat);
	}

	/* Allow concurrent queries to update this feature space. */
	LockRelease(&tag, ExclusiveLock, false);

	cur_classes = list_delete_int(cur_classes, query_context.query_hash);

	RemoveFromQueryEnv(queryDesc);

end:
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	/*
	 * standard_ExecutorEnd clears the queryDesc->planstate. After this point no
	 * one operation with the plan can be made.
	 */
}

/*
 * Store into query environment field AQO data related to the query.
 * We introduce this machinery to avoid problems with subqueries, induced by
 * top-level query.
 */
static void
StoreToQueryEnv(QueryDesc *queryDesc)
{
	EphemeralNamedRelation	enr;
	int	qcsize = sizeof(QueryContextData);
	MemoryContext	oldCxt;

	oldCxt = MemoryContextSwitchTo(AQOMemoryContext);
	enr = palloc0(sizeof(EphemeralNamedRelationData));
	if (queryDesc->queryEnv == NULL)
		queryDesc->queryEnv = create_queryEnv();

	enr->md.name = AQOPrivateData;
	enr->md.enrtuples = 0;
	enr->md.enrtype = 0;
	enr->md.reliddesc = InvalidOid;
	enr->md.tupdesc = NULL;

	enr->reldata = palloc0(qcsize);
	memcpy(enr->reldata, &query_context, qcsize);

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

	njoins = 0;
	planstate_tree_walker(queryDesc->planstate, calculateJoinNum, &njoins);

	oldCxt = MemoryContextSwitchTo(AQOMemoryContext);
	enr = palloc0(sizeof(EphemeralNamedRelationData));
	if (queryDesc->queryEnv == NULL)
			queryDesc->queryEnv = create_queryEnv();

	enr->md.name = PlanStateInfo;
	enr->md.enrtuples = 0;
	enr->md.enrtype = 0;
	enr->md.reliddesc = InvalidOid;
	enr->md.tupdesc = NULL;
	enr->reldata = palloc0(sizeof(int));
	memcpy(enr->reldata, &njoins, sizeof(int));
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

static void
RemoveFromQueryEnv(QueryDesc *queryDesc)
{
	EphemeralNamedRelation enr = get_ENR(queryDesc->queryEnv, AQOPrivateData);
	unregister_ENR(queryDesc->queryEnv, AQOPrivateData);
	pfree(enr->reldata);
	pfree(enr);

	/* Remove the plan state internals */
	enr = get_ENR(queryDesc->queryEnv, PlanStateInfo);
	unregister_ENR(queryDesc->queryEnv, PlanStateInfo);
	pfree(enr->reldata);
	pfree(enr);
}

void
print_node_explain(ExplainState *es, PlanState *ps, Plan *plan)
{
	int wrkrs = 1;
	double error = -1.;
	AQOPlanNode *aqo_node;

	/* Extension, which took a hook early can be executed early too. */
	if (prev_ExplainOneNode_hook)
		prev_ExplainOneNode_hook(es, ps, plan);

	if (es->format != EXPLAIN_FORMAT_TEXT)
		/* Only text format is supported. */
		return;

	if (!aqo_show_details || !plan || !ps)
		goto explain_end;

	aqo_node = get_aqo_plan_node(plan, false);

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
	/* XXX: Do we really have situations than plan is NULL? */
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

	if (!aqo_show_details)
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
		if (aqo_show_hash)
			ExplainPropertyInteger("Query hash", NULL,
									query_context.query_hash, es);
		ExplainPropertyInteger("JOINS", NULL, njoins, es);
	}
}
