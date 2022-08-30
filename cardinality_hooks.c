/*
 *******************************************************************************
 *
 *	CARDINALITY ESTIMATION HOOKS
 *
 * This functions controls cardinality prediction in query optimization.
 * If use_aqo flag is false, then hooks just call default postgresql
 * cardinality estimator. Otherwise, they try to use AQO cardinality
 * prediction engine.
 * If use_aqo flag in true, hooks generate set of all clauses and all
 * absolute relids used in the relation being built and pass this
 * information to predict_for_relation function. Also these hooks compute
 * and pass to predict_for_relation marginal cardinalities for clauses.
 * If predict_for_relation returns non-negative value, then hooks assume it
 * to be true cardinality for given relation. Negative returned value means
 * refusal to predict cardinality. In this case hooks also use default
 * postgreSQL cardinality estimator.
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/cardinality_hooks.c
 *
 */

#include "postgres.h"

#include "aqo.h"
#include "cardinality_hooks.h"
#include "hash.h"
#include "machine_learning.h"
#include "path_utils.h"

estimate_num_groups_hook_type prev_estimate_num_groups_hook = NULL;

double predicted_ppi_rows;
double fss_ppi_hash;


/*
 * Calls standard set_baserel_rows_estimate or its previous hook.
 */
static void
default_set_baserel_rows_estimate(PlannerInfo *root, RelOptInfo *rel)
{
	if (prev_set_baserel_rows_estimate_hook)
		prev_set_baserel_rows_estimate_hook(root, rel);
	else
		set_baserel_rows_estimate_standard(root, rel);
}

/*
 * Calls standard get_parameterized_baserel_size or its previous hook.
 */
static double
default_get_parameterized_baserel_size(PlannerInfo *root,
											RelOptInfo *rel,
											List *param_clauses)
{
	if (prev_get_parameterized_baserel_size_hook)
		return prev_get_parameterized_baserel_size_hook(root, rel, param_clauses);
	else
		return get_parameterized_baserel_size_standard(root, rel, param_clauses);
}

/*
 * Calls standard get_parameterized_joinrel_size or its previous hook.
 */
static double
default_get_parameterized_joinrel_size(PlannerInfo *root,
											RelOptInfo *rel,
											Path *outer_path,
											Path *inner_path,
											SpecialJoinInfo *sjinfo,
											List *restrict_clauses)
{
	if (prev_get_parameterized_joinrel_size_hook)
		return prev_get_parameterized_joinrel_size_hook(root, rel,
														outer_path,
														inner_path,
														sjinfo,
														restrict_clauses);
	else
		return get_parameterized_joinrel_size_standard(root, rel,
													   outer_path,
													   inner_path,
													   sjinfo,
													   restrict_clauses);
}

/*
 * Calls standard set_joinrel_size_estimates or its previous hook.
 */
static void
default_set_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel,
										RelOptInfo *outer_rel,
										RelOptInfo *inner_rel,
										SpecialJoinInfo *sjinfo,
										List *restrictlist)
{
	if (prev_set_joinrel_size_estimates_hook)
		prev_set_joinrel_size_estimates_hook(root, rel,
											 outer_rel,
											 inner_rel,
											 sjinfo,
											 restrictlist);
	else
		set_joinrel_size_estimates_standard(root, rel,
											outer_rel,
											inner_rel,
											sjinfo,
											restrictlist);
}

static double
default_estimate_num_groups(PlannerInfo *root, List *groupExprs,
							Path *subpath, RelOptInfo *grouped_rel,
							List **pgset)
{
	double input_rows = subpath->rows;

	if (prev_estimate_num_groups_hook != NULL)
			return (*prev_estimate_num_groups_hook)(root, groupExprs,
													subpath,
													grouped_rel,
													pgset);
	else
		return estimate_num_groups(root, groupExprs, input_rows, pgset);
}

/*
 * Our hook for setting baserel rows estimate.
 * Extracts clauses, their selectivities and list of relation relids and
 * passes them to predict_for_relation.
 */
void
aqo_set_baserel_rows_estimate(PlannerInfo *root, RelOptInfo *rel)
{
	double			predicted;
	RangeTblEntry  *rte;
	RelSortOut		rels = {NIL, NIL};
	List		   *selectivities = NULL;
	List		   *clauses;
	int				fss = 0;
	MemoryContext old_ctx_m;

	if (IsQueryDisabled())
		/* Fast path. */
		goto default_estimator;

	old_ctx_m = MemoryContextSwitchTo(AQOPredictMemCtx);

	if (query_context.use_aqo || query_context.learn_aqo)
		selectivities = get_selectivities(root, rel->baserestrictinfo, 0,
										  JOIN_INNER, NULL);

	if (!query_context.use_aqo)
	{
		MemoryContextSwitchTo(old_ctx_m);
		goto default_estimator;
	}

	rte = planner_rt_fetch(rel->relid, root);
	if (rte && OidIsValid(rte->relid))
	{
		/* Predict for a plane table. */
		Assert(rte->eref && rte->eref->aliasname);
		get_list_of_relids(root, rel->relids, &rels);
	}

	clauses = aqo_get_clauses(root, rel->baserestrictinfo);
	predicted = predict_for_relation(clauses, selectivities, rels.signatures,
									 &fss);
	rel->fss_hash = fss;

	/* Return to the caller's memory context. */
	MemoryContextSwitchTo(old_ctx_m);

	if (predicted >= 0)
	{
		rel->rows = predicted;
		rel->predicted_cardinality = predicted;
		return;
	}

default_estimator:
	rel->predicted_cardinality = -1.;
	default_set_baserel_rows_estimate(root, rel);
}


void
ppi_hook(ParamPathInfo *ppi)
{
	if (IsQueryDisabled())
		return;

	ppi->predicted_ppi_rows = predicted_ppi_rows;
	ppi->fss_ppi_hash = fss_ppi_hash;
}

/*
 * Our hook for estimating parameterized baserel rows estimate.
 * Extracts clauses (including parametrization ones), their selectivities
 * and list of relation relids and passes them to predict_for_relation.
 */
double
aqo_get_parameterized_baserel_size(PlannerInfo *root,
								   RelOptInfo *rel,
								   List *param_clauses)
{
	double		predicted;
	RangeTblEntry *rte = NULL;
	RelSortOut	rels = {NIL, NIL};
	List	   *allclauses = NULL;
	List	   *selectivities = NULL;
	ListCell   *l;
	ListCell   *l2;
	int			nargs;
	int		   *args_hash;
	int		   *eclass_hash;
	int			current_hash;
	int			fss = 0;
	MemoryContext oldctx;

	if (IsQueryDisabled())
		/* Fast path */
		goto default_estimator;

	oldctx = MemoryContextSwitchTo(AQOPredictMemCtx);

	if (query_context.use_aqo || query_context.learn_aqo)
	{

		selectivities = list_concat(
							get_selectivities(root, param_clauses, rel->relid,
											  JOIN_INNER, NULL),
							get_selectivities(root, rel->baserestrictinfo,
											  rel->relid,
											  JOIN_INNER, NULL));

		/* Make specific copy of clauses with mutated subplans */
		allclauses = list_concat(aqo_get_clauses(root, param_clauses),
								 aqo_get_clauses(root, rel->baserestrictinfo));

		rte = planner_rt_fetch(rel->relid, root);
		get_eclasses(allclauses, &nargs, &args_hash, &eclass_hash);

		forboth(l, allclauses, l2, selectivities)
		{
			current_hash = get_clause_hash(
										((RestrictInfo *) lfirst(l))->clause,
										   nargs, args_hash, eclass_hash);
			cache_selectivity(current_hash, rel->relid, rte->relid,
							  *((double *) lfirst(l2)));
		}
	}

	if (!query_context.use_aqo)
	{
		MemoryContextSwitchTo(oldctx);

		goto default_estimator;
	}

	if (rte && OidIsValid(rte->relid))
	{
		/* Predict for a plane table. */
		Assert(rte->eref && rte->eref->aliasname);
		get_list_of_relids(root, rel->relids, &rels);
	}

	predicted = predict_for_relation(allclauses, selectivities, rels.signatures, &fss);

	/* Return to the caller's memory context */
	MemoryContextSwitchTo(oldctx);

	predicted_ppi_rows = predicted;
	fss_ppi_hash = fss;

	if (predicted >= 0)
		return predicted;

default_estimator:
	return default_get_parameterized_baserel_size(root, rel, param_clauses);
}

/*
 * Our hook for setting joinrel rows estimate.
 * Extracts clauses, their selectivities and list of relation relids and
 * passes them to predict_for_relation.
 */
void
aqo_set_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel,
							   RelOptInfo *outer_rel,
							   RelOptInfo *inner_rel,
							   SpecialJoinInfo *sjinfo,
							   List *restrictlist)
{
	double		predicted;
	RelSortOut rels = {NIL, NIL};
	List	   *outer_clauses;
	List	   *inner_clauses;
	List	   *allclauses;
	List	   *selectivities;
	List	   *inner_selectivities;
	List	   *outer_selectivities;
	List	   *current_selectivities = NULL;
	int			fss = 0;
	MemoryContext old_ctx_m;

	if (IsQueryDisabled())
		/* Fast path */
		goto default_estimator;

	old_ctx_m = MemoryContextSwitchTo(AQOPredictMemCtx);

	if (query_context.use_aqo || query_context.learn_aqo)
		current_selectivities = get_selectivities(root, restrictlist, 0,
												  sjinfo->jointype, sjinfo);
	if (!query_context.use_aqo)
	{
		MemoryContextSwitchTo(old_ctx_m);
		goto default_estimator;
	}

	get_list_of_relids(root, rel->relids, &rels);
	outer_clauses = get_path_clauses(outer_rel->cheapest_total_path, root,
									 &outer_selectivities);
	inner_clauses = get_path_clauses(inner_rel->cheapest_total_path, root,
									 &inner_selectivities);
	allclauses = list_concat(aqo_get_clauses(root, restrictlist),
							 list_concat(outer_clauses, inner_clauses));
	selectivities = list_concat(current_selectivities,
								list_concat(outer_selectivities,
											inner_selectivities));

	predicted = predict_for_relation(allclauses, selectivities, rels.signatures,
									 &fss);

	/* Return to the caller's memory context */
	MemoryContextSwitchTo(old_ctx_m);

	rel->fss_hash = fss;

	if (predicted >= 0)
	{
		rel->predicted_cardinality = predicted;
		rel->rows = predicted;
		return;
	}

default_estimator:
	rel->predicted_cardinality = -1;
	default_set_joinrel_size_estimates(root, rel,
									   outer_rel, inner_rel,
									   sjinfo, restrictlist);
}

/*
 * Our hook for estimating parameterized joinrel rows estimate.
 * Extracts clauses (including parametrization ones), their selectivities
 * and list of relation relids and passes them to predict_for_relation.
 */
double
aqo_get_parameterized_joinrel_size(PlannerInfo *root,
								   RelOptInfo *rel,
								   Path *outer_path,
								   Path *inner_path,
								   SpecialJoinInfo *sjinfo,
								   List *clauses)
{
	double		predicted;
	RelSortOut	rels = {NIL, NIL};
	List	   *outer_clauses;
	List	   *inner_clauses;
	List	   *allclauses;
	List	   *selectivities;
	List	   *inner_selectivities;
	List	   *outer_selectivities;
	List	   *current_selectivities = NULL;
	int			fss = 0;
	MemoryContext old_ctx_m;

	if (IsQueryDisabled())
		/* Fast path */
		goto default_estimator;

	old_ctx_m = MemoryContextSwitchTo(AQOPredictMemCtx);

	if (query_context.use_aqo || query_context.learn_aqo)
		current_selectivities = get_selectivities(root, clauses, 0,
												  sjinfo->jointype, sjinfo);

	if (!query_context.use_aqo)
	{
		MemoryContextSwitchTo(old_ctx_m);
		goto default_estimator;
	}

	get_list_of_relids(root, rel->relids, &rels);
	outer_clauses = get_path_clauses(outer_path, root, &outer_selectivities);
	inner_clauses = get_path_clauses(inner_path, root, &inner_selectivities);
	allclauses = list_concat(aqo_get_clauses(root, clauses),
							 list_concat(outer_clauses, inner_clauses));
	selectivities = list_concat(current_selectivities,
								list_concat(outer_selectivities,
											inner_selectivities));

	predicted = predict_for_relation(allclauses, selectivities, rels.signatures,
									 &fss);
	/* Return to the caller's memory context */
	MemoryContextSwitchTo(old_ctx_m);

	predicted_ppi_rows = predicted;
	fss_ppi_hash = fss;

	if (predicted >= 0)
		return predicted;

default_estimator:
	return default_get_parameterized_joinrel_size(root, rel,
												  outer_path, inner_path,
												  sjinfo, clauses);
}

static double
predict_num_groups(PlannerInfo *root, Path *subpath, List *group_exprs,
				   int *fss)
{
	int			child_fss = 0;
	double		prediction;
	OkNNrdata	data;

	if (subpath->parent->predicted_cardinality > 0.)
		/* A fast path. Here we can use a fss hash of a leaf. */
		child_fss = subpath->parent->fss_hash;
	else
	{
		RelSortOut rels = {NIL, NIL};
		List	  *clauses;
		List	  *selectivities = NIL;

		get_list_of_relids(root, subpath->parent->relids, &rels);
		clauses = get_path_clauses(subpath, root, &selectivities);
		(void) predict_for_relation(clauses, selectivities, rels.signatures,
									&child_fss);
	}

	*fss = get_grouped_exprs_hash(child_fss, group_exprs);
	memset(&data, 0, sizeof(OkNNrdata));

	if (!load_fss_ext(query_context.fspace_hash, *fss, &data, NULL, true))
		return -1;

	Assert(data.rows == 1);
	prediction = exp(data.targets[0]);
	return (prediction <= 0) ? -1 : prediction;
}

double
aqo_estimate_num_groups_hook(PlannerInfo *root, List *groupExprs,
							 Path *subpath, RelOptInfo *grouped_rel,
							 List **pgset)
{
	int fss;
	double predicted;
	MemoryContext old_ctx_m;

	if (!query_context.use_aqo)
		goto default_estimator;

	if (pgset || groupExprs == NIL)
		/* XXX: Don't support some GROUPING options */
		goto default_estimator;

	if (prev_estimate_num_groups_hook != NULL)
		elog(WARNING, "AQO replaced another estimator of a groups number");

	if (groupExprs == NIL)
		return 1.0;

	old_ctx_m = MemoryContextSwitchTo(AQOPredictMemCtx);

	predicted = predict_num_groups(root, subpath, groupExprs, &fss);
	if (predicted > 0.)
	{
		grouped_rel->predicted_cardinality = predicted;
		grouped_rel->rows = predicted;
		grouped_rel->fss_hash = fss;
		MemoryContextSwitchTo(old_ctx_m);
		return predicted;
	}
	else
		/*
		 * Some nodes AQO doesn't know yet, some nodes are ignored by AQO
		 * permanently - as an example, SubqueryScan.
		 */
		grouped_rel->predicted_cardinality = -1;

	MemoryContextSwitchTo(old_ctx_m);

default_estimator:
	return default_estimate_num_groups(root, groupExprs, subpath, grouped_rel,
									   pgset);
}
