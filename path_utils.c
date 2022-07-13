/*
 *******************************************************************************
 *
 *	EXTRACTING PATH INFORMATION UTILITIES
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/path_utils.c
 *
 */
#include "postgres.h"

#include "access/relation.h"
#include "nodes/readfuncs.h"
#include "optimizer/optimizer.h"
#include "path_utils.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#include "aqo.h"
#include "hash.h"


/*
 * Hook on creation of a plan node. We need to store AQO-specific data to
 * support learning stage.
 */
create_plan_hook_type prev_create_plan_hook = NULL;

create_upper_paths_hook_type prev_create_upper_paths_hook = NULL;

static AQOPlanNode DefaultAQOPlanNode =
{
	.node.type = T_ExtensibleNode,
	.node.extnodename = AQO_PLAN_NODE,
	.had_path = false,
	.rels = NULL,
	.clauses = NIL,
	.selectivities = NIL,
	.grouping_exprs = NIL,
	.jointype = -1,
	.parallel_divisor = -1.,
	.was_parametrized = false,
	.fss = INT_MAX,
	.prediction = -1
};

static AQOPlanNode *
create_aqo_plan_node()
{
	AQOPlanNode *node = (AQOPlanNode *) newNode(sizeof(AQOPlanNode),
															T_ExtensibleNode);

	memcpy(node, &DefaultAQOPlanNode, sizeof(AQOPlanNode));
	node->rels = palloc(sizeof(RelSortOut));
	node->rels->hrels = NIL;
	node->rels->signatures = NIL;
	return node;
}

/*
 * Extract an AQO node from the plan private field.
 * If no one node was found, return pointer to the default value or allocate new
 * node (with default value) according to 'create' field.
 * Can't return NULL value at all.
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
			return &DefaultAQOPlanNode;

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
static uint64
hashTempTupleDesc(TupleDesc desc)
{
	uint64		s;
	int			i;

	s = hash_combine(0, hash_uint32(desc->natts));

	for (i = 0; i < desc->natts; ++i)
	{
		const char *attname = NameStr(TupleDescAttr(desc, i)->attname);
		uint64		s1;

		s = hash_combine64(s, hash_uint32(TupleDescAttr(desc, i)->atttypid));
		s1 = hash_bytes_extended((const unsigned char *) attname, strlen(attname), 0);
		s = hash_combine64(s, s1);
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

		entry = planner_rt_fetch(index, root);

		if (!OidIsValid(entry->relid))
		{
			/* Invalid oid */
			hashes = lappend_uint64(hashes, (UINT64_MAX / 7));
			continue;
		}

		htup = SearchSysCache1(RELOID, ObjectIdGetDatum(entry->relid));
		if (!HeapTupleIsValid(htup))
			elog(PANIC, "cache lookup failed for reloid %u", entry->relid);

		classForm = (Form_pg_class) GETSTRUCT(htup);

		if (classForm->relpersistence == RELPERSISTENCE_TEMP)
		{
			/* The case of temporary table */

			Relation trel = relation_open(entry->relid, NoLock);
			TupleDesc tdesc = RelationGetDescr(trel);

			hashes = lappend_uint64(hashes, hashTempTupleDesc(tdesc));
			relation_close(trel, NoLock);
		}
		else
		{
			/* The case of regular table */
			relname = quote_qualified_identifier(
				get_namespace_name(get_rel_namespace(entry->relid)),
				classForm->relrewrite ?
										get_rel_name(classForm->relrewrite) :
										NameStr(classForm->relname));
			hashes = lappend_uint64(hashes, DatumGetInt64(hash_any_extended(
											(unsigned char *) relname,
											strlen(relname), 0)));

			hrels = lappend_oid(hrels, entry->relid);
			pfree(relname);
		}

		ReleaseSysCache(htup);
	}

	rels->hrels = list_concat(rels->hrels, hrels);
	rels->signatures = list_concat(rels->signatures, hashes);
	return;
}

/*
 * Search for any subplans or initplans.
 * if subplan is found, replace it by the feature space value of this subplan.
 */
static Node *
subplan_hunter(Node *node, void *context)
{
	if (node == NULL)
		/* Continue recursion in other subtrees. */
		return false;

	if (IsA(node, SubPlan))
	{
		SubPlan		*splan = (SubPlan *) node;
		PlannerInfo	*root = (PlannerInfo *) context;
		PlannerInfo	*subroot;
		RelOptInfo	*upper_rel;
		A_Const		*fss;

		subroot = (PlannerInfo *) list_nth(root->glob->subroots,
										   splan->plan_id - 1);
		upper_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);

		Assert(list_length(upper_rel->ext_nodes) == 1);
		Assert(IsA((Node *) linitial(upper_rel->ext_nodes), A_Const));

		fss = (A_Const *) linitial(upper_rel->ext_nodes);
		return (Node *) copyObject(fss);
	}
	return expression_tree_mutator(node, subplan_hunter, context);
}

/*
 * Get independent copy of the clauses list.
 * During this operation clauses could be changed and we couldn't walk across
 * this list next.
 */
List *
aqo_get_clauses(PlannerInfo *root, List *restrictlist)
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
		case T_GatherMergePath:
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
		case T_ProjectSetPath:
			return get_path_clauses(((ProjectSetPath *) path)->subpath, root,
									selectivities);
			break;
		case T_SortPath:
			return get_path_clauses(((SortPath *) path)->subpath, root,
									selectivities);
			break;
		case T_IncrementalSortPath:
			{
				IncrementalSortPath *p = (IncrementalSortPath *) path;
				return get_path_clauses(p->spath.subpath, root,
									selectivities);
			}
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
		case T_SubqueryScanPath:
			/* Recursing into Subquery we must use subroot */
			Assert(path->parent->subroot != NULL);
			return get_path_clauses(((SubqueryScanPath *) path)->subpath,
									path->parent->subroot,
									selectivities);
			break;
		case T_ModifyTablePath:
		{
			ListCell *lc;

			foreach (lc, ((ModifyTablePath *) path)->subpaths)
			{
				Path *subpath = lfirst(lc);

				cur = list_concat(cur, list_copy(
					get_path_clauses(subpath, root, selectivities)));
				cur_sel = list_concat(cur_sel, *selectivities);
			}
			cur = list_concat(cur, aqo_get_clauses(root,
											path->parent->baserestrictinfo));
			*selectivities = list_concat(cur_sel,
										 get_selectivities(root,
											path->parent->baserestrictinfo,
											0, JOIN_INNER, NULL));
			return cur;
		}
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

				cur = list_concat(cur, list_copy(
					get_path_clauses(subpath, root, selectivities)));
				cur_sel = list_concat(cur_sel, *selectivities);
			}
			cur = list_concat(cur, aqo_get_clauses(root,
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
			cur = list_concat(list_copy(path->parent->baserestrictinfo),
							  path->param_info ?
							  path->param_info->ppi_clauses : NIL);
			if (path->param_info)
				cur_sel = get_selectivities(root, cur, path->parent->relid,
											JOIN_INNER, NULL);
			else
				cur_sel = get_selectivities(root, cur, 0, JOIN_INNER, NULL);
			*selectivities = cur_sel;
			cur = aqo_get_clauses(root, cur);
			return cur;
			break;
	}
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
 * Converts path info into plan node for collecting it after query execution.
 */
void
aqo_create_plan_hook(PlannerInfo *root, Path *src, Plan **dest)
{
	bool			is_join_path;
	Plan		   *plan = *dest;
	AQOPlanNode	   *node;

	if (prev_create_plan_hook)
		prev_create_plan_hook(root, src, dest);

	if (!query_context.use_aqo && !query_context.learn_aqo)
		return;

	is_join_path = (src->type == T_NestPath || src->type == T_MergePath ||
					src->type == T_HashPath);

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
		node->clauses = aqo_get_clauses(root, ((JoinPath *) src)->joinrestrictinfo);
		node->jointype = ((JoinPath *) src)->jointype;
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
		get_list_of_relids(root, ap->subpath->parent->relids, node->rels);
		node->jointype = JOIN_INNER;
	}
	else if (is_appropriate_path(src))
	{
		node->clauses = list_concat(
			aqo_get_clauses(root, src->parent->baserestrictinfo),
				src->param_info ? aqo_get_clauses(root, src->param_info->ppi_clauses) : NIL);
		node->jointype = JOIN_INNER;
	}

	get_list_of_relids(root, src->parent->relids, node->rels);

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

	/* Copy static fields in one command */
	memcpy(new, old, sizeof(AQOPlanNode));

	/* These lists couldn't contain AQO nodes. Use basic machinery */
	new->rels = palloc(sizeof(RelSortOut));
	new->rels->hrels = list_copy(old->rels->hrels);
	new->rels->signatures = list_copy_uint64(old->rels->signatures);

	new->clauses = copyObject(old->clauses);
	new->grouping_exprs = copyObject(old->grouping_exprs);
	new->selectivities = copyObject(old->selectivities);
	enew = (ExtensibleNode *) new;
}

static bool
AQOnodeEqual(const struct ExtensibleNode *a, const struct ExtensibleNode *b)
{
	return false;
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

/* Write a float field --- caller must give format to define precision */
#define WRITE_FLOAT_FIELD(fldname,format) \
	appendStringInfo(str, " :" CppAsString(fldname) " " format, node->fldname)

static void
AQOnodeOut(struct StringInfoData *str, const struct ExtensibleNode *enode)
{
	AQOPlanNode *node = (AQOPlanNode *) enode;

	Assert(0);
	WRITE_BOOL_FIELD(had_path);
	WRITE_NODE_FIELD(rels);
	WRITE_NODE_FIELD(clauses);
	WRITE_NODE_FIELD(selectivities);
	WRITE_NODE_FIELD(grouping_exprs);

	WRITE_ENUM_FIELD(jointype, JoinType);
	WRITE_FLOAT_FIELD(parallel_divisor, "%.5f");
	WRITE_BOOL_FIELD(was_parametrized);

	/* For Adaptive optimization DEBUG purposes */
	WRITE_INT_FIELD(fss);
	WRITE_FLOAT_FIELD(prediction, "%.0f");
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

static void
AQOnodeRead(struct ExtensibleNode *enode)
{
	AQOPlanNode *local_node = (AQOPlanNode *) enode;
	const char	*token;
	int			length;

	Assert(0);
	READ_BOOL_FIELD(had_path);
	READ_NODE_FIELD(rels);
	READ_NODE_FIELD(clauses);
	READ_NODE_FIELD(selectivities);
	READ_NODE_FIELD(grouping_exprs);

	READ_ENUM_FIELD(jointype, JoinType);
	READ_FLOAT_FIELD(parallel_divisor);
	READ_BOOL_FIELD(was_parametrized);

	/* For Adaptive optimization DEBUG purposes */
	READ_INT_FIELD(fss);
	READ_FLOAT_FIELD(prediction);
}

static const ExtensibleNodeMethods method =
{
	.extnodename = AQO_PLAN_NODE,
	.node_size = sizeof(AQOPlanNode),
	.nodeCopy =  AQOnodeCopy,
	.nodeEqual = AQOnodeEqual,
	.nodeOut = AQOnodeOut,
	.nodeRead = AQOnodeRead
};

void
RegisterAQOPlanNodeMethods(void)
{
	RegisterExtensibleNodeMethods(&method);
}

/*
 * Hook for create_upper_paths_hook
 *
 * Assume, that we are last in the chain of path creators.
 */
void
aqo_store_upper_signature_hook(PlannerInfo *root,
							   UpperRelationKind stage,
							   RelOptInfo *input_rel,
							   RelOptInfo *output_rel,
							   void *extra)
{
	A_Const	   *fss_node = makeNode(A_Const);
	RelSortOut	rels = {NIL, NIL};
	List	   *clauses;
	List	   *selectivities;

	if (prev_create_upper_paths_hook)
		(*prev_create_upper_paths_hook)(root, stage, input_rel, output_rel, extra);

	if (!query_context.use_aqo && !query_context.learn_aqo && !force_collect_stat)
		/* Includes 'disabled query' state. */
		return;

	if (stage != UPPERREL_FINAL)
		return;

	set_cheapest(input_rel);
	clauses = get_path_clauses(input_rel->cheapest_total_path,
													root, &selectivities);
	get_list_of_relids(root, input_rel->relids, &rels);
	fss_node->val.type = T_Integer;
	fss_node->location = -1;
	fss_node->val.val.ival = get_fss_for_object(rels.signatures, clauses, NIL,
												NULL, NULL);
	output_rel->ext_nodes = lappend(output_rel->ext_nodes, (void *) fss_node);
}
