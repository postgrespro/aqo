/*
 *******************************************************************************
 *
 *	EXTRACTING PATH INFORMATION UTILITIES
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2023, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/path_utils.c
 *
 */
#include "postgres.h"

#include "access/relation.h"
#include "nodes/readfuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/planmain.h"
#include "path_utils.h"
#include "storage/lmgr.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "common/shortest_dec.h"

#include "aqo.h"
#include "hash.h"

#include "postgres_fdw.h"


static AQOPlanNode DefaultAQOPlanNode =
{
	.node.type = T_ExtensibleNode,
	.node.extnodename = AQO_PLAN_NODE,
	.had_path = false,
	.rels.hrels = NIL,
	.rels.signatures = NIL,
	.clauses = NIL,
	.selectivities = NIL,
	.grouping_exprs = NIL,
	.jointype = -1,
	.parallel_divisor = -1.,
	.was_parametrized = false,
	.fss = INT_MAX,
	.prediction = -1.
};

/*
 * Auxiliary list for relabel equivalence classes
 * from pointers to the serial numbers - indexes of this list.
 * XXX: Maybe it's need to use some smart data structure such a HTAB?
 * It must be allocated in AQOCacheMemCtx.
 */
List *aqo_eclass_collector = NIL;

/*
 * Hook on creation of a plan node. We need to store AQO-specific data to
 * support learning stage.
 */
static create_plan_hook_type			aqo_create_plan_next		= NULL;

/*static create_upper_paths_hook_type	aqo_create_upper_paths_next	= NULL;*/


/* Return a copy of the given list of AQOClause structs */
static List *
copy_aqo_clauses(List *src)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, src)
	{
		AQOClause *old = (AQOClause *) lfirst(lc);
		AQOClause *new = palloc(sizeof(AQOClause));

		memcpy(new, old, sizeof(AQOClause));
		new->clause = copyObject(old->clause);

		result = lappend(result, (void *) new);
	}

	return result;
}

static AQOPlanNode *
create_aqo_plan_node()
{
	AQOPlanNode *node = (AQOPlanNode *) newNode(sizeof(AQOPlanNode),
															T_ExtensibleNode);
	Assert(node != NULL);
	memcpy(node, &DefaultAQOPlanNode, sizeof(AQOPlanNode));
	return node;
}

AQOConstNode *
create_aqo_const_node(AQOConstType type, int fss)
{
	AQOConstNode *node = (AQOConstNode *) newNode(sizeof(AQOConstNode),
															T_ExtensibleNode);
	Assert(node != NULL);
	node->node.extnodename = AQO_CONST_NODE;
	node->type = type;
	node->fss = fss;
	return node;
}

/* Ensure that it's postgres_fdw's foreign server oid */
static bool
is_postgres_fdw_server(Oid serverid)
{
	ForeignServer *server;
	ForeignDataWrapper *fdw;

	if (!OidIsValid(serverid))
		return false;

	server = GetForeignServerExtended(serverid, FSV_MISSING_OK);
	if (!server)
		return false;

	fdw = GetForeignDataWrapperExtended(server->fdwid, FDW_MISSING_OK);
	if (!fdw || !fdw->fdwname)
		return false;

	if (strcmp(fdw->fdwname, "postgres_fdw") != 0)
		return false;

	return true;
}

/*
 * Extract an AQO node from the plan private field.
 * If no one node was found, return pointer to the default value or return NULL.
 */
AQOPlanNode *
get_aqo_plan_node(Plan *plan, bool create)
{
	AQOPlanNode *node = NULL;
	ListCell	*lc;

	foreach(lc, plan->ext_nodes)
	{
		AQOPlanNode *candidate = (AQOPlanNode *) lfirst(lc);

		if (!IsA(candidate, ExtensibleNode))
			continue;

		if (strcmp(candidate->node.extnodename, AQO_PLAN_NODE) != 0)
			continue;

		node = candidate;
		break;
	}

	if (node == NULL)
	{
		if (!create)
			return NULL;

		node = create_aqo_plan_node();
		plan->ext_nodes = lappend(plan->ext_nodes, node);
	}

	Assert(node);
	return node;
}

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
 * Based on the hashTupleDesc() routine
 */
static uint32
hashTempTupleDesc(TupleDesc desc)
{
	uint32		s;
	int			i;

	s = hash_combine(0, hash_uint32(desc->natts));

	for (i = 0; i < desc->natts; ++i)
	{
		const char *attname = NameStr(TupleDescAttr(desc, i)->attname);
		uint32		s1;

		s = hash_combine(s, hash_uint32(TupleDescAttr(desc, i)->atttypid));
		s1 = hash_bytes((const unsigned char *) attname, strlen(attname));
		s = hash_combine(s, s1);
	}
	return s;
}

/*
 * Get list of relation indexes and prepare list of permanent table reloids,
 * list of temporary table reloids (can be changed between query launches) and
 * array of table signatures.
 */
void
get_list_of_relids(PlannerInfo *root, Relids relids, RelSortOut *rels)
{
	int				index;
	RangeTblEntry  *entry;
	List		   *hrels = NIL;
	List		   *hashes = NIL;

	if (relids == NULL)
		return;

	index = -1;
	while ((index = bms_next_member(relids, index)) >= 0)
	{
		HeapTuple		htup;
		Form_pg_class	classForm;
		char		   *relname = NULL;
		Oid				relrewrite;
		char			relpersistence;

		entry = planner_rt_fetch(index, root);

		if (!OidIsValid(entry->relid))
		{
			/* TODO: Explain this logic. */
			hashes = lappend_int(hashes, INT32_MAX / 3);
			continue;
		}

		htup = SearchSysCache1(RELOID, ObjectIdGetDatum(entry->relid));
		if (!HeapTupleIsValid(htup))
			elog(PANIC, "cache lookup failed for reloid %u", entry->relid);

		/* Copy the fields from syscache and release the slot as quickly as possible. */
		classForm = (Form_pg_class) GETSTRUCT(htup);
		relpersistence = classForm->relpersistence;
		relrewrite = classForm->relrewrite;
		relname = pstrdup(NameStr(classForm->relname));
		ReleaseSysCache(htup);

		if (relpersistence == RELPERSISTENCE_TEMP)
		{
			/* The case of temporary table */

			Relation	trel;
			TupleDesc	tdesc;

			trel = relation_open(entry->relid, NoLock);
			tdesc = RelationGetDescr(trel);
			Assert(CheckRelationLockedByMe(trel, AccessShareLock, true));
			hashes = lappend_int(hashes, hashTempTupleDesc(tdesc));
			relation_close(trel, NoLock);
		}
		else
		{
			/* The case of regular table */
			relname = quote_qualified_identifier(
						get_namespace_name(get_rel_namespace(entry->relid)),
							relrewrite ? get_rel_name(relrewrite) : relname);

			hashes = lappend_int(hashes, DatumGetInt32(hash_any(
											(unsigned char *) relname,
											strlen(relname))));

			hrels = lappend_oid(hrels, entry->relid);
		}
	}

	rels->hrels = list_concat(rels->hrels, hrels);
	rels->signatures = list_concat(rels->signatures, hashes);
	return;
}

/*
 * Search for any subplans or initplans.
 * if subplan is found, replace it by zero Const.
 */
static Node *
subplan_hunter(Node *node, void *context)
{
	if (node == NULL)
		/* Continue recursion in other subtrees. */
		return false;

	if (IsA(node, SubPlan))
	{
		/* TODO: use fss of SubPlan here */
		return (Node *) create_aqo_const_node(AQO_NODE_SUBPLAN, 0);
	}
	return expression_tree_mutator(node, subplan_hunter, context);
}

/*
 * Get independent copy of the clauses list.
 * During this operation clauses could be changed and we couldn't walk across
 * this list next.
 */
static List *
aqo_get_raw_clauses(PlannerInfo *root, List *restrictlist)
{
	List		*clauses = NIL;
	ListCell	*lc;

	foreach(lc, restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		rinfo = copyObject(rinfo);
		rinfo->clause = (Expr *) expression_tree_mutator((Node *) rinfo->clause,
														 subplan_hunter,
														 (void *) root);
		clauses = lappend(clauses, (void *) rinfo);
	}
	return clauses;
}

static int
get_eclass_index(EquivalenceClass *ec)
{
	ListCell	   *lc;
	int				i = 0;
	MemoryContext	old_ctx;

	if (ec == NULL)
		return -1;

	/* Get the top of merged eclasses */
	while(ec->ec_merged)
		ec = ec->ec_merged;

	foreach (lc, aqo_eclass_collector)
	{
		if (lfirst(lc) == ec)
			break;
		i++;
	}

	old_ctx = MemoryContextSwitchTo(AQOCacheMemCtx);
	if (i == list_length(aqo_eclass_collector))
		aqo_eclass_collector = lappend(aqo_eclass_collector, ec);
	MemoryContextSwitchTo(old_ctx);

	return i;
}

static List *
copy_aqo_clauses_from_rinfo(List *src)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, src)
	{
		RestrictInfo *old = (RestrictInfo *) lfirst(lc);
		AQOClause *new = palloc(sizeof(AQOClause));

		new->clause = copyObject(old->clause);
		new->norm_selec = old->norm_selec;
		new->outer_selec = old->outer_selec;

		new->left_ec = get_eclass_index(old->left_ec);
		new->right_ec = get_eclass_index(old->right_ec);

		new->is_eq_clause = (old->left_ec != NULL || old->left_ec != NULL);

		result = lappend(result, (void *) new);
	}

	return result;
}

/*
 * Return copy of clauses returned from the aqo_get_raw_clause() routine
 * and convert it into AQOClause struct.
 */
List *
aqo_get_clauses(PlannerInfo *root, List *restrictlist)
{
	List		*clauses = aqo_get_raw_clauses(root, restrictlist);
	List		*result = copy_aqo_clauses_from_rinfo(clauses);

	list_free_deep(clauses);
	return result;
}

/*
 * Returns a list of all used clauses for the given path.
 * Also returns selectivities for the clauses to 'selectivities' variable.
 * The returned list of the selectivities is a copy and therefore
 * may be modified without corruption of the input data.
 */
static List *
get_path_clauses_recurse(Path *path, PlannerInfo *root, List **selectivities)
{
	List	   *inner;
	List	   *inner_sel = NIL;
	List	   *outer;
	List	   *outer_sel = NIL;
	List	   *cur = NIL;
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
			cur = list_concat(cur, ((JoinPath *) path)->joinrestrictinfo);

			/* Not quite correct to avoid sjinfo, but we believe in caching */
			cur_sel = get_selectivities(root, cur, 0,
										((JoinPath *) path)->jointype,
										NULL);

			outer = get_path_clauses_recurse(((JoinPath *) path)->outerjoinpath, root,
									 &outer_sel);
			inner = get_path_clauses_recurse(((JoinPath *) path)->innerjoinpath, root,
									 &inner_sel);
			*selectivities = list_concat(cur_sel,
										 list_concat(outer_sel, inner_sel));
			return list_concat(cur, list_concat(outer, inner));
			break;
		case T_UniquePath:
			return get_path_clauses_recurse(((UniquePath *) path)->subpath, root,
									selectivities);
			break;
		case T_GatherPath:
		case T_GatherMergePath:
			return get_path_clauses_recurse(((GatherPath *) path)->subpath, root,
									selectivities);
			break;
		case T_MaterialPath:
			return get_path_clauses_recurse(((MaterialPath *) path)->subpath, root,
									selectivities);
			break;
		case T_MemoizePath:
			return get_path_clauses_recurse(((MemoizePath *) path)->subpath, root,
									selectivities);
			break;
		case T_ProjectionPath:
			return get_path_clauses_recurse(((ProjectionPath *) path)->subpath, root,
									selectivities);
			break;
		case T_ProjectSetPath:
			return get_path_clauses_recurse(((ProjectSetPath *) path)->subpath, root,
									selectivities);
			break;
		case T_SortPath:
			return get_path_clauses_recurse(((SortPath *) path)->subpath, root,
									selectivities);
			break;
		case T_IncrementalSortPath:
			{
				IncrementalSortPath *p = (IncrementalSortPath *) path;
				return get_path_clauses_recurse(p->spath.subpath, root,
									selectivities);
			}
			break;
		case T_GroupPath:
			return get_path_clauses_recurse(((GroupPath *) path)->subpath, root,
									selectivities);
			break;
		case T_UpperUniquePath:
			return get_path_clauses_recurse(((UpperUniquePath *) path)->subpath, root,
									selectivities);
			break;
		case T_AggPath:
			return get_path_clauses_recurse(((AggPath *) path)->subpath, root,
									selectivities);
			break;
		case T_GroupingSetsPath:
			return get_path_clauses_recurse(((GroupingSetsPath *) path)->subpath, root,
									selectivities);
			break;
		case T_WindowAggPath:
			return get_path_clauses_recurse(((WindowAggPath *) path)->subpath, root,
									selectivities);
			break;
		case T_SetOpPath:
			return get_path_clauses_recurse(((SetOpPath *) path)->subpath, root,
									selectivities);
			break;
		case T_LockRowsPath:
			return get_path_clauses_recurse(((LockRowsPath *) path)->subpath, root,
									selectivities);
			break;
		case T_LimitPath:
			return get_path_clauses_recurse(((LimitPath *) path)->subpath, root,
									selectivities);
			break;
		case T_SubqueryScanPath:
			/* Recursing into Subquery we must use subroot */
			Assert(path->parent->subroot != NULL);
			return get_path_clauses_recurse(((SubqueryScanPath *) path)->subpath,
									path->parent->subroot,
									selectivities);
			break;
		case T_ModifyTablePath:
			return get_path_clauses_recurse(((ModifyTablePath *) path)->subpath, root,
									selectivities);
			break;
		/* TODO: RecursiveUnionPath */
		case T_AppendPath:
		case T_MergeAppendPath:
		{
			ListCell *lc;

			 /*
			  * It isn't a safe style, but we use the only subpaths field that is
			  * the first at both Append and MergeAppend nodes.
			  */
			foreach (lc, ((AppendPath *) path)->subpaths)
			{
				Path *subpath = lfirst(lc);

				cur = list_concat(cur,
					get_path_clauses_recurse(subpath, root, selectivities));
				cur_sel = list_concat(cur_sel, *selectivities);
			}
			cur = list_concat(cur, aqo_get_raw_clauses(root,
											path->parent->baserestrictinfo));
			*selectivities = list_concat(cur_sel,
										 get_selectivities(root,
											path->parent->baserestrictinfo,
											0, JOIN_INNER, NULL));
			return cur;
		}
			break;
		case T_ForeignPath:
			/* The same as in the default case */
		default:
			cur = list_concat(list_concat(cur, path->parent->baserestrictinfo),
							  path->param_info ?
							  path->param_info->ppi_clauses : NIL);
			if (path->param_info)
				cur_sel = get_selectivities(root, cur, path->parent->relid,
											JOIN_INNER, NULL);
			else
				cur_sel = get_selectivities(root, cur, 0, JOIN_INNER, NULL);
			*selectivities = cur_sel;
			cur = aqo_get_raw_clauses(root, cur);
			return cur;
			break;
	}
}

/*
 * Returns a list of AQOClauses for the given path, which is a copy
 * of the clauses returned from the get_path_clauses_recurse() routine.
 * Also returns selectivities for the clauses to 'selectivities' variable.
 * Both returned lists are copies and therefore may be modified without
 * corruption of the input data.
 */
List *
get_path_clauses(Path *path, PlannerInfo *root, List **selectivities)
{
	return copy_aqo_clauses_from_rinfo(
		get_path_clauses_recurse(path, root, selectivities));
}

/*
 * Some of paths are kind of utility path. I mean, It isn't corresponding to
 * specific RelOptInfo node. So, it should be omitted in process of clauses
 * gathering to avoid duplication of the same clauses.
 * XXX: only a dump plug implemented for now.
 */
static bool
is_appropriate_path(Path *path)
{
	bool appropriate = true;

	switch (path->type)
	{
		case T_SortPath:
		case T_IncrementalSortPath:
		case T_MemoizePath:
		case T_GatherPath:
		case T_GatherMergePath:
			appropriate = false;
			break;
		default:
			break;
	}

	return appropriate;
}

/*
 * Add AQO data into the plan node, if necessary.
 *
 * The necesssary case is when AQO is learning on this query, used for a
 * prediction (and we will need the data to show prediction error at the end) or
 * just to gather a plan statistics.
 * Don't switch here to any AQO-specific memory contexts, because we should
 * store AQO prediction in the same context, as the plan. So, explicitly free
 * all unneeded data.
 */
static void
aqo_create_plan(PlannerInfo *root, Path *src, Plan **dest)
{
	bool			is_join_path;
	Plan		   *plan = *dest;
	AQOPlanNode	   *node;

	if (aqo_create_plan_next)
		(*aqo_create_plan_next)(root, src, dest);

	if (!query_context.use_aqo && !query_context.learn_aqo &&
		!query_context.collect_stat)
		return;

	is_join_path = (src->type == T_NestPath || src->type == T_MergePath ||
					src->type == T_HashPath ||
					(src->type == T_ForeignPath && IS_JOIN_REL(src->parent)));

	node = get_aqo_plan_node(plan, true);

	if (node->had_path)
	{
		/*
		 * The convention is that any extension that sets had_path is also
		 * responsible for setting path_clauses, path_jointype, path_relids,
		 * path_parallel_workers, and was_parameterized.
		 */
		return;
	}

	if (is_join_path)
	{
		if (IsA(src, ForeignPath))
		{
			PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) src->parent->fdw_private;
			List	*restrictclauses = NIL;

			if (!fpinfo)
				return;

			/* We have to ensure that this is postgres_fdw ForeignPath */
			if (!is_postgres_fdw_server(src->parent->serverid))
				return;

			restrictclauses = list_concat(restrictclauses, fpinfo->joinclauses);
			restrictclauses = list_concat(restrictclauses, fpinfo->remote_conds);
			restrictclauses = list_concat(restrictclauses, fpinfo->local_conds);

			node->clauses = aqo_get_clauses(root, restrictclauses);
			node->jointype = fpinfo->jointype;

			list_free(restrictclauses);
		}
		else
		{
			node->clauses = aqo_get_clauses(root, ((JoinPath *) src)->joinrestrictinfo);
			node->jointype = ((JoinPath *) src)->jointype;
		}
	}
	else if (IsA(src, AggPath))
	/* Aggregation node must store grouping clauses. */
	{
		AggPath *ap = (AggPath *) src;

		/* Get TLE's from child target list corresponding to the list of exprs. */
		List *groupExprs = get_sortgrouplist_exprs(ap->groupClause,
												(*dest)->lefttree->targetlist);
		/* Copy bare expressions for further AQO learning case. */
		node->grouping_exprs = copyObject(groupExprs);
		get_list_of_relids(root, ap->subpath->parent->relids, &node->rels);
		node->jointype = JOIN_INNER;
	}
	else if (is_appropriate_path(src))
	{
		node->clauses = list_concat(
			aqo_get_clauses(root, src->parent->baserestrictinfo),
				src->param_info ? aqo_get_clauses(root, src->param_info->ppi_clauses) : NIL);
		node->jointype = JOIN_INNER;
	}

	get_list_of_relids(root, src->parent->relids, &node->rels);

	if (src->parallel_workers > 0)
		node->parallel_divisor = get_parallel_divisor(src);
	node->was_parametrized = (src->param_info != NULL);

	if (src->param_info)
	{
		node->prediction = src->param_info->predicted_ppi_rows;
		node->fss = src->param_info->fss_ppi_hash;
	}
	else
	{
		/*
		 * In the case of forced stat gathering AQO must store fss as well as
		 * parallel divisor. Negative predicted cardinality field will be a sign
		 * that it is not a prediction, just statistics.
		 */
		node->prediction = src->parent->predicted_cardinality;
		node->fss = src->parent->fss_hash;
	}

	node->had_path = true;
}

static void
AQOnodeCopy(struct ExtensibleNode *enew, const struct ExtensibleNode *eold)
{
	AQOPlanNode *new = (AQOPlanNode *) enew;
	AQOPlanNode *old = (AQOPlanNode *) eold;

	Assert(IsA(old, ExtensibleNode));
	Assert(strcmp(old->node.extnodename, AQO_PLAN_NODE) == 0);
	Assert(new && old);

	/*
	 * Copy static fields in one command.
	 * But do not copy fields of the old->node.
	 * Elsewise, we can use pointers that will be freed.
	 * For example, it is old->node.extnodename.
	 */
	memcpy(&new->had_path, &old->had_path, sizeof(AQOPlanNode) - offsetof(AQOPlanNode, had_path));

	/* These lists couldn't contain AQO nodes. Use basic machinery */
	new->rels.hrels = list_copy(old->rels.hrels);
	new->rels.signatures = list_copy(old->rels.signatures);

	new->clauses = copy_aqo_clauses(old->clauses);
	new->grouping_exprs = copyObject(old->grouping_exprs);
	new->selectivities = copyObject(old->selectivities);
	enew = (ExtensibleNode *) new;
}

static bool
AQOnodeEqual(const struct ExtensibleNode *a, const struct ExtensibleNode *b)
{
	return false;
}

static void
AQOconstCopy(struct ExtensibleNode *enew, const struct ExtensibleNode *eold)
{
	AQOConstNode *new = (AQOConstNode *) enew;
	AQOConstNode *old = (AQOConstNode *) eold;

	Assert(IsA(old, ExtensibleNode));
	Assert(strcmp(old->node.extnodename, AQO_CONST_NODE) == 0);
	Assert(new && old);

	new->type = old->type;
	new->fss = old->fss;
	enew = (ExtensibleNode *) new;
}

static bool
AQOconstEqual(const struct ExtensibleNode *a, const struct ExtensibleNode *b)
{
	return false;
}

/*
 * Convert a double value, attempting to ensure the value is preserved exactly.
 */
static void
outDouble(StringInfo str, double d)
{
	char		buf[DOUBLE_SHORTEST_DECIMAL_LEN];

	double_to_shortest_decimal_buf(d, buf);
	appendStringInfoString(str, buf);
}

#define WRITE_INT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %s", \
					 booltostr(node->fldname))

#define WRITE_NODE_FIELD(fldname) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 outNode(str, node->fldname))

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", \
					 (int) node->fldname)

/* Write a float field */
#define WRITE_FLOAT_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outDouble(str, node->fldname))

/* The start part of a custom list writer */
#define WRITE_CUSTOM_LIST_START(fldname) \
	{ \
		appendStringInfo(str, " :N_" CppAsString(fldname) " %d ", \
			list_length(node->fldname)); \
		/* Serialize this list like an array */ \
		if (list_length(node->fldname)) \
		{ \
			ListCell	*lc; \
			appendStringInfo(str, "("); \
			foreach (lc, node->fldname)

/* The end part of a custom list writer */
#define WRITE_CUSTOM_LIST_END() \
			appendStringInfo(str, " )"); \
		} \
		else \
			appendStringInfo(str, "<>"); \
	}

/* Write a list of int values */
#define WRITE_INT_LIST(fldname) \
	WRITE_CUSTOM_LIST_START(fldname) \
	{ \
		int val = lfirst_int(lc); \
		appendStringInfo(str, " %d", val); \
	} \
	WRITE_CUSTOM_LIST_END()

/* Write a list of AQOClause values */
#define WRITE_AQOCLAUSE_LIST(fldname) \
	WRITE_CUSTOM_LIST_START(clauses) \
	{ \
		AQOClause *node = lfirst(lc); \
		/* Serialize this struct like a node */ \
		appendStringInfo(str, " {"); \
		WRITE_NODE_FIELD(clause); \
		WRITE_FLOAT_FIELD(norm_selec); \
		WRITE_FLOAT_FIELD(outer_selec); \
		WRITE_INT_FIELD(left_ec); \
		WRITE_INT_FIELD(right_ec); \
		WRITE_BOOL_FIELD(is_eq_clause); \
		appendStringInfo(str, " }"); \
	} \
	WRITE_CUSTOM_LIST_END()

/*
 * Serialize AQO plan node to a string.
 *
 * Some extensions may manipulate by parts of serialized plan too.
 */
static void
AQOnodeOut(struct StringInfoData *str, const struct ExtensibleNode *enode)
{
	AQOPlanNode *node = (AQOPlanNode *) enode;

	WRITE_BOOL_FIELD(had_path);

	WRITE_NODE_FIELD(rels.hrels);
	WRITE_INT_LIST(rels.signatures);

	WRITE_AQOCLAUSE_LIST(clauses);

	WRITE_NODE_FIELD(selectivities);
	WRITE_NODE_FIELD(grouping_exprs);
	WRITE_ENUM_FIELD(jointype, JoinType);

	WRITE_FLOAT_FIELD(parallel_divisor);
	WRITE_BOOL_FIELD(was_parametrized);

	WRITE_INT_FIELD(fss);
	WRITE_FLOAT_FIELD(prediction);
}

/*
 * Serialize AQO const node to a string.
 *
 * Some extensions may manipulate by parts of serialized plan too.
 */
static void
AQOconstOut(struct StringInfoData *str, const struct ExtensibleNode *enode)
{
	AQOConstNode *node = (AQOConstNode *) enode;

	WRITE_ENUM_FIELD(type, AQOConstType);
	WRITE_INT_FIELD(fss);
}

/* Read an integer field (anything written as ":fldname %d") */
#define READ_INT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoi(token)

/* Read an enumerated-type field that was written as an integer code */
#define READ_ENUM_FIELD(fldname, enumtype) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = (enumtype) atoi(token)

/* Read a float field */
#define READ_FLOAT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atof(token)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = strtobool(token)

/* Read a Node field */
#define READ_NODE_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = nodeRead(NULL, 0)

/* The start part of a custom list reader */
#define READ_CUSTOM_LIST_START() \
	{ \
		int	counter; \
		token = pg_strtok(&length); /* skip the name */ \
		token = pg_strtok(&length); \
		counter = atoi(token); \
		token = pg_strtok(&length); /* left bracket "(" */ \
		if (length) \
		{ \
			for (int i = 0; i < counter; i++)

/* The end part of a custom list reader */
#define READ_CUSTOM_LIST_END(fldname) \
			token = pg_strtok(&length); /* right bracket ")" */ \
		} \
		else \
			local_node->fldname = NIL; \
	}

/* Read a list of int values */
#define READ_INT_LIST(fldname) \
	READ_CUSTOM_LIST_START() \
	{ \
		int val; \
		token = pg_strtok(&length); \
		val = atoi(token); \
		local_node->fldname = lappend_int( \
			local_node->fldname, val); \
	} \
	READ_CUSTOM_LIST_END(fldname)

/* Read a list of AQOClause values */
#define READ_AQOCLAUSE_LIST(fldname) \
	READ_CUSTOM_LIST_START() \
	{ \
		/* copy to use in the inner blocks of code */ \
		AQOPlanNode *node_copy = local_node; \
		AQOClause  *local_node = palloc(sizeof(AQOClause)); \
		token = pg_strtok(&length); /* left bracket "{" */ \
		READ_NODE_FIELD(clause); \
		READ_FLOAT_FIELD(norm_selec); \
		READ_FLOAT_FIELD(outer_selec); \
		READ_INT_FIELD(left_ec); \
		READ_INT_FIELD(right_ec); \
		READ_BOOL_FIELD(is_eq_clause); \
		token = pg_strtok(&length); /* right bracket "}" */ \
		node_copy->fldname = lappend(node_copy->fldname, local_node); \
	} \
	READ_CUSTOM_LIST_END(fldname)

/*
 * Deserialize AQO plan node from a string to internal representation.
 *
 * Should work in coherence with AQOnodeOut().
 */
static void
AQOnodeRead(struct ExtensibleNode *enode)
{
	AQOPlanNode *local_node = (AQOPlanNode *) enode;
	const char	*token;
	int			length;

	READ_BOOL_FIELD(had_path);

	READ_NODE_FIELD(rels.hrels);
	READ_INT_LIST(rels.signatures);

	READ_AQOCLAUSE_LIST(clauses);

	READ_NODE_FIELD(selectivities);
	READ_NODE_FIELD(grouping_exprs);
	READ_ENUM_FIELD(jointype, JoinType);

	READ_FLOAT_FIELD(parallel_divisor);
	READ_BOOL_FIELD(was_parametrized);

	READ_INT_FIELD(fss);
	READ_FLOAT_FIELD(prediction);
}

/*
 * Deserialize AQO const node from a string to internal representation.
 *
 * Should work in coherence with AQOconstOut().
 */
static void
AQOconstRead(struct ExtensibleNode *enode)
{
	AQOConstNode   *local_node = (AQOConstNode *) enode;
	const char	   *token;
	int				length;

	READ_ENUM_FIELD(type, AQOConstType);
	READ_INT_FIELD(fss);
}

static const ExtensibleNodeMethods aqo_node_method =
{
	.extnodename = AQO_PLAN_NODE,
	.node_size = sizeof(AQOPlanNode),
	.nodeCopy =  AQOnodeCopy,
	.nodeEqual = AQOnodeEqual,
	.nodeOut = AQOnodeOut,
	.nodeRead = AQOnodeRead
};

static const ExtensibleNodeMethods aqo_const_method =
{
	.extnodename = AQO_CONST_NODE,
	.node_size = sizeof(AQOConstNode),
	.nodeCopy =  AQOconstCopy,
	.nodeEqual = AQOconstEqual,
	.nodeOut = AQOconstOut,
	.nodeRead = AQOconstRead
};

void
RegisterAQOPlanNodeMethods(void)
{
	RegisterExtensibleNodeMethods(&aqo_node_method);
	RegisterExtensibleNodeMethods(&aqo_const_method);
}

/*
 * Warning! This function does not word properly.
 * Because value of Const nodes removed by hash routine.
 *
 * Hook for create_upper_paths_hook
 *
 * Assume, that we are last in the chain of path creators.
 */
/*static void
aqo_store_upper_signature(PlannerInfo *root,
						  UpperRelationKind stage,
						  RelOptInfo *input_rel,
						  RelOptInfo *output_rel,
						  void *extra)
{
	A_Const	   *fss_node = makeNode(A_Const);
	RelSortOut	rels = {NIL, NIL};
	List	   *clauses;
	List	   *selectivities;

	if (aqo_create_upper_paths_next)
		(*aqo_create_upper_paths_next)(root, stage, input_rel, output_rel, extra);

	if (!query_context.use_aqo && !query_context.learn_aqo && !force_collect_stat)
		/ * Includes 'disabled query' state. * /
		return;

	if (stage != UPPERREL_FINAL)
		return;

	set_cheapest(input_rel);
	clauses = get_path_clauses(input_rel->cheapest_total_path,
													root, &selectivities);
	get_list_of_relids(root, input_rel->relids, &rels);
	fss_node->val.ival.type = T_Integer;
	fss_node->location = -1;
	fss_node->val.ival.ival = get_fss_for_object(rels.signatures, clauses, NIL,
												NULL, NULL);
	output_rel->ext_nodes = lappend(output_rel->ext_nodes, (void *) fss_node);
}*/

void
aqo_path_utils_init(void)
{
	aqo_create_plan_next				= create_plan_hook;
	create_plan_hook					= aqo_create_plan;

	/*aqo_create_upper_paths_next			= create_upper_paths_hook;
	create_upper_paths_hook				= aqo_store_upper_signature;*/
}
