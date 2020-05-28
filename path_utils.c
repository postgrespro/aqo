/*
 *******************************************************************************
 *
 *	EXTRACTING PATH INFORMATION UTILITIES
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2020, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/path_utils.c
 *
 */

#include "aqo.h"
#include "optimizer/optimizer.h"

/*
 * Returns list of marginal selectivities using as an arguments for each clause
 * (root, clause, 0, jointype, NULL).
 * That is not quite correct for parameterized baserel and foreign key join
 * cases, but nevertheless that is bearable.
 */
List *
get_selectivities(PlannerInfo *root,
				  List *clauses,
				  int varRelids,
				  JoinType jointype,
				  SpecialJoinInfo *sjinfo)
{
	List	   *res = NIL;
	ListCell   *l;
	double	   *elem;

	foreach(l, clauses)
	{
		elem = palloc(sizeof(*elem));
		*elem = clause_selectivity(root, lfirst(l), varRelids,
								   jointype, sjinfo);
		res = lappend(res, elem);
	}

	return res;
}

/*
 * Transforms given relids from path optimization stage format to list of
 * an absolute (independent on query optimization context) relids.
 */
List *
get_list_of_relids(PlannerInfo *root, Relids relids)
{
	int			i;
	RangeTblEntry *entry;
	List	   *l = NIL;

	if (relids == NULL)
		return NIL;

	i = -1;
	while ((i = bms_next_member(relids, i)) >= 0)
	{
		entry = planner_rt_fetch(i, root);
		l = lappend_int(l, entry->relid);
	}
	return l;
}

/*
 * For given path returns the list of all clauses used in it.
 * Also returns selectivities for the clauses throw the selectivities variable.
 * Both clauses and selectivities returned lists are copies and therefore
 * may be modified without corruption of the input data.
 */
List *
get_path_clauses(Path *path, PlannerInfo *root, List **selectivities)
{
	List	   *inner;
	List	   *inner_sel = NIL;
	List	   *outer;
	List	   *outer_sel = NIL;
	List	   *cur;
	List	   *cur_sel = NIL;

	Assert(selectivities != NULL);
	*selectivities = NIL;

	if (path == NULL)
		return NIL;

	switch (path->type)
	{
		case T_NestPath:
		case T_MergePath:
		case T_HashPath:
			cur = ((JoinPath *) path)->joinrestrictinfo;

			/* Not quite correct to avoid sjinfo, but we believe in caching */
			cur_sel = get_selectivities(root, cur, 0,
										((JoinPath *) path)->jointype,
										NULL);

			outer = get_path_clauses(((JoinPath *) path)->outerjoinpath, root,
									 &outer_sel);
			inner = get_path_clauses(((JoinPath *) path)->innerjoinpath, root,
									 &inner_sel);
			*selectivities = list_concat(cur_sel,
										 list_concat(outer_sel, inner_sel));
			return list_concat(list_copy(cur), list_concat(outer, inner));
			break;
		case T_UniquePath:
			return get_path_clauses(((UniquePath *) path)->subpath, root,
									selectivities);
			break;
		case T_GatherPath:
			return get_path_clauses(((GatherPath *) path)->subpath, root,
									selectivities);
			break;
		case T_MaterialPath:
			return get_path_clauses(((MaterialPath *) path)->subpath, root,
									selectivities);
			break;
		case T_ProjectionPath:
			return get_path_clauses(((ProjectionPath *) path)->subpath, root,
									selectivities);
			break;
		case T_SortPath:
			return get_path_clauses(((SortPath *) path)->subpath, root,
									selectivities);
			break;
		case T_GroupPath:
			return get_path_clauses(((GroupPath *) path)->subpath, root,
									selectivities);
			break;
		case T_UpperUniquePath:
			return get_path_clauses(((UpperUniquePath *) path)->subpath, root,
									selectivities);
			break;
		case T_AggPath:
			return get_path_clauses(((AggPath *) path)->subpath, root,
									selectivities);
			break;
		case T_GroupingSetsPath:
			return get_path_clauses(((GroupingSetsPath *) path)->subpath, root,
									selectivities);
			break;
		case T_WindowAggPath:
			return get_path_clauses(((WindowAggPath *) path)->subpath, root,
									selectivities);
			break;
		case T_SetOpPath:
			return get_path_clauses(((SetOpPath *) path)->subpath, root,
									selectivities);
			break;
		case T_LockRowsPath:
			return get_path_clauses(((LockRowsPath *) path)->subpath, root,
									selectivities);
			break;
		case T_LimitPath:
			return get_path_clauses(((LimitPath *) path)->subpath, root,
									selectivities);
			break;
		default:
			cur = list_concat(list_copy(path->parent->baserestrictinfo),
							  path->param_info ?
							  list_copy(path->param_info->ppi_clauses) : NIL);
			if (path->param_info)
				cur_sel = get_selectivities(root, cur, path->parent->relid,
											JOIN_INNER, NULL);
			else
				cur_sel = get_selectivities(root, cur, 0, JOIN_INNER, NULL);
			*selectivities = cur_sel;
			return cur;
			break;
	}
}
