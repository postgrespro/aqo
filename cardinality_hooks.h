#ifndef CARDINALITY_HOOKS_H
#define CARDINALITY_HOOKS_H

#include "optimizer/planner.h"
#include "utils/selfuncs.h"

extern estimate_num_groups_hook_type prev_estimate_num_groups_hook;


/* Cardinality estimation hooks */
extern void aqo_set_baserel_rows_estimate(PlannerInfo *root, RelOptInfo *rel);
extern double aqo_get_parameterized_baserel_size(PlannerInfo *root,
												 RelOptInfo *rel,
												 List *param_clauses);
extern void aqo_set_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel,
										   RelOptInfo *outer_rel,
										   RelOptInfo *inner_rel,
										   SpecialJoinInfo *sjinfo,
										   List *restrictlist);
extern double aqo_get_parameterized_joinrel_size(PlannerInfo *root,
												 RelOptInfo *rel,
												 Path *outer_path,
												 Path *inner_path,
												 SpecialJoinInfo *sjinfo,
												 List *restrict_clauses);
extern double aqo_estimate_num_groups_hook(PlannerInfo *root, List *groupExprs,
										   Path *subpath,
										   RelOptInfo *grouped_rel,
										   List **pgset,
										   EstimationInfo *estinfo);

#endif /* CARDINALITY_HOOKS_H */
