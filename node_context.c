/*
 * node_context.c
 *		Node Context Extractor for AQO
 *
 * Extracts clause conditions, selectivities, estimated/actual cardinality,
 * space_hash, and relation names from PostgreSQL plan nodes.
 * Data is collected during planning (via create_plan_hook) and updated
 * during execution (via ExecutorEnd). Results are flushed to the
 * aqo_node_context table using SPI.
 *
 * Copyright (c) 2024, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/node_context.c
 */
#include "postgres.h"

#include "executor/execdesc.h"
#include "executor/spi.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"

#include "aqo.h"
#include "node_context.h"

/* GUC variable — controlled via aqo.nce_enabled */
bool aqo_nce_enabled = false;

/* Recursion guard: prevent NCE from capturing its own SPI queries */
static bool nce_in_flush = false;

/*
 * Backend-local list of NceCollectedNode pointers.
 * Allocated in TopMemoryContext to survive across planning → execution boundary.
 * Reset after each query's ExecutorEnd (or on error).
 */
static List *nce_collected_nodes = NIL;

/*
 * Helper: convert a NodeTag to a human-readable string.
 */
static const char *
nce_node_type_str(NodeTag tag)
{
	switch (tag)
	{
		case T_SeqScan:				return "SeqScan";
		case T_IndexScan:			return "IndexScan";
		case T_IndexOnlyScan:		return "IndexOnlyScan";
		case T_BitmapHeapScan:		return "BitmapHeapScan";
		case T_BitmapIndexScan:		return "BitmapIndexScan";
		case T_TidScan:				return "TidScan";
		case T_SubqueryScan:		return "SubqueryScan";
		case T_FunctionScan:		return "FunctionScan";
		case T_ValuesScan:			return "ValuesScan";
		case T_CteScan:				return "CteScan";
		case T_WorkTableScan:		return "WorkTableScan";
		case T_ForeignScan:			return "ForeignScan";
		case T_NestLoop:			return "NestLoop";
		case T_MergeJoin:			return "MergeJoin";
		case T_HashJoin:			return "HashJoin";
		case T_Material:			return "Material";
		case T_Sort:				return "Sort";
		case T_Agg:					return "Agg";
		case T_Group:				return "Group";
		case T_Unique:				return "Unique";
		case T_Hash:				return "Hash";
		case T_Limit:				return "Limit";
		case T_Append:				return "Append";
		case T_MergeAppend:			return "MergeAppend";
		case T_Result:				return "Result";
		case T_Gather:				return "Gather";
		case T_GatherMerge:			return "GatherMerge";
		default:					return "Other";
	}
}

/*
 * Helper: convert JoinType to string.
 */
static const char *
nce_join_type_str(JoinType jt)
{
	switch (jt)
	{
		case JOIN_INNER:	return "INNER";
		case JOIN_LEFT:		return "LEFT";
		case JOIN_FULL:		return "FULL";
		case JOIN_RIGHT:	return "RIGHT";
		case JOIN_SEMI:		return "SEMI";
		case JOIN_ANTI:		return "ANTI";
		default:			return "OTHER";
	}
}

/*
 * Helper: Try to produce human-readable SQL from a clause expression.
 * Uses deparse_expression with the provided context.
 * Falls back to nodeToString if deparsing fails.
 * Returns a palloc'd string.
 */
static char *
nce_deparse_clause(Expr *clause, List *dpcontext)
{
	char *result = NULL;

	if (clause == NULL)
		return pstrdup("<null>");

	PG_TRY();
	{
		if (dpcontext != NIL)
			result = deparse_expression((Node *) clause, dpcontext, true, false);
		else
			result = nodeToString((Node *) clause);
	}
	PG_CATCH();
	{
		FlushErrorState();
		result = NULL;
	}
	PG_END_TRY();

	if (result == NULL)
	{
		PG_TRY();
		{
			result = nodeToString((Node *) clause);
		}
		PG_CATCH();
		{
			FlushErrorState();
			result = pstrdup("<unable to deparse>");
		}
		PG_END_TRY();
	}

	return result ? result : pstrdup("<null>");
}

/*
 * nce_collect_plan_node
 *
 * Called from aqo_create_plan() after the AQOPlanNode is fully populated.
 * Extracts clause text, selectivities, estimated cardinality, space_hash,
 * and relation names.
 *
 * Clause deparsing uses a fake PlannedStmt built from root->parse->rtable
 * to construct a full deparse context. This gives human-readable SQL even
 * for multi-table join queries, without needing the real PlannedStmt
 * (which isn't available yet during planning).
 */
void
nce_collect_plan_node(PlannerInfo *root, Path *src, Plan *plan,
					  AQOPlanNode *aqo_node)
{
	MemoryContext	oldctx;
	NceCollectedNode *entry;
	ListCell	   *lc;
	int				i;
	List		   *dpcontext = NIL;

	if (!aqo_nce_enabled || !aqo_node->had_path)
		return;

	/* Allocate the entry in TopMemoryContext for cross-phase survival */
	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	entry = (NceCollectedNode *) palloc0(sizeof(NceCollectedNode));
	entry->query_hash = query_context.query_hash;
	entry->space_hash = aqo_node->fss;
	entry->node_type = nodeTag(plan);
	entry->jointype = aqo_node->jointype;
	entry->estimated_cardinality = (aqo_node->prediction > 0.)
									? aqo_node->prediction
									: src->parent->rows;
	entry->actual_cardinality = -1.0;	/* filled during execution */
	entry->matched = false;
	entry->nclauses = 0;
	entry->nrels = 0;

	MemoryContextSwitchTo(oldctx);

	/*
	 * Build deparse context from root->parse->rtable using a temporary
	 * PlannedStmt. deparse_context_for_plan_tree() only reads pstmt->rtable,
	 * so a minimal fake PlannedStmt on the stack is sufficient.
	 * This gives us full multi-table name resolution for deparsing.
	 */
	if (root->parse && root->parse->rtable != NIL)
	{
		PG_TRY();
		{
			PlannedStmt fake_pstmt;
			List	   *rtable_names;

			memset(&fake_pstmt, 0, sizeof(PlannedStmt));
			fake_pstmt.type = T_PlannedStmt;
			fake_pstmt.rtable = root->parse->rtable;

			rtable_names = select_rtable_names_for_explain(
								root->parse->rtable, NULL);
			dpcontext = deparse_context_for_plan_tree(&fake_pstmt,
													  rtable_names);
		}
		PG_CATCH();
		{
			FlushErrorState();
			dpcontext = NIL;
		}
		PG_END_TRY();
	}

	/*
	 * Extract clause texts and selectivities from AQOClause list.
	 * Deparse each clause immediately while planner memory is still valid.
	 */
	i = 0;
	foreach(lc, aqo_node->clauses)
	{
		AQOClause  *clause = (AQOClause *) lfirst(lc);
		char	   *clause_str;

		if (i >= NCE_MAX_CLAUSES)
			break;

		/* Deparse clause to human-readable text using full context */
		clause_str = nce_deparse_clause(clause->clause, dpcontext);

		/* Copy result into TopMemoryContext */
		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		entry->clause_texts[i] = pstrdup(clause_str);

		/* Use norm_selec for INNER joins, outer_selec otherwise */
		if (aqo_node->jointype == JOIN_INNER)
			entry->selectivities[i] = clause->norm_selec;
		else
			entry->selectivities[i] = clause->outer_selec >= 0
										? clause->outer_selec
										: clause->norm_selec;
		MemoryContextSwitchTo(oldctx);

		i++;
	}

	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	entry->nclauses = i;

	/*
	 * Extract relation names from rels.hrels (list of Oid).
	 */
	i = 0;
	foreach(lc, aqo_node->rels.hrels)
	{
		Oid		reloid = lfirst_oid(lc);
		char   *relname;

		if (i >= NCE_MAX_RELS)
			break;

		relname = get_rel_name(reloid);
		entry->relation_names[i] = relname ? pstrdup(relname) : pstrdup("<unknown>");
		i++;
	}
	entry->nrels = i;

	/*
	 * Skip entries that reference AQO internal tables to avoid capturing
	 * our own SPI queries and SELECT FROM aqo_* tables.
	 */
	{
		bool is_aqo_internal = false;
		int j;
		for (j = 0; j < entry->nrels; j++)
		{
			if (strncmp(entry->relation_names[j], "aqo_", 4) == 0)
			{
				is_aqo_internal = true;
				break;
			}
		}
		if (is_aqo_internal)
		{
			/* Free the entry and don't add to collected list */
			for (j = 0; j < entry->nclauses; j++)
				if (entry->clause_texts[j]) pfree(entry->clause_texts[j]);
			for (j = 0; j < entry->nrels; j++)
				if (entry->relation_names[j]) pfree(entry->relation_names[j]);
			pfree(entry);
			MemoryContextSwitchTo(oldctx);
			return;
		}
	}

	nce_collected_nodes = lappend(nce_collected_nodes, entry);
	MemoryContextSwitchTo(oldctx);
}

/*
 * nce_update_actual_cardinality
 *
 * Called from learnOnPlanState() during ExecutorEnd.
 * Matches collected nodes by (space_hash, node_type) and sets actual_cardinality.
 * Uses first-unmatched strategy for duplicates.
 */
void
nce_update_actual_cardinality(int space_hash, NodeTag node_type,
							  double actual_rows)
{
	ListCell *lc;

	foreach(lc, nce_collected_nodes)
	{
		NceCollectedNode *entry = (NceCollectedNode *) lfirst(lc);

		if (!entry->matched &&
			entry->space_hash == space_hash &&
			entry->node_type == node_type)
		{
			entry->actual_cardinality = actual_rows;
			entry->matched = true;
			return;
		}
	}
}

/*
 * nce_flush_to_table
 *
 * Flush all collected node context entries to the aqo_node_context table
 * using SPI. Called from aqo_ExecutorEnd after learning.
 *
 * Clause texts were already deparsed during planning (in nce_collect_plan_node),
 * so this function just performs the SPI INSERT.
 *
 * Uses PG_TRY/PG_CATCH so failures don't break normal query execution.
 */
void
nce_flush_to_table(QueryDesc *queryDesc)
{
	ListCell   *lc;

	if (nce_collected_nodes == NIL || nce_in_flush)
		return;

	nce_in_flush = true;

	PG_TRY();
	{
		int		ret;

		ret = SPI_connect();
		if (ret != SPI_OK_CONNECT)
		{
			elog(WARNING, "[AQO NCE] SPI_connect failed: %d", ret);
			nce_in_flush = false;
			nce_reset_collected();
			return;
		}
		PushActiveSnapshot(GetTransactionSnapshot());

		foreach(lc, nce_collected_nodes)
		{
			NceCollectedNode *entry = (NceCollectedNode *) lfirst(lc);
			StringInfoData	buf;
			StringInfoData	sel_buf;
			StringInfoData	rel_buf;
			int				i;

			initStringInfo(&buf);
			initStringInfo(&sel_buf);
			initStringInfo(&rel_buf);

			/*
			 * Build the selectivities array literal: ARRAY[0.1, 0.5, ...]
			 */
			appendStringInfoString(&sel_buf, "ARRAY[");
			for (i = 0; i < entry->nclauses; i++)
			{
				if (i > 0)
					appendStringInfoString(&sel_buf, ", ");
				appendStringInfo(&sel_buf, "%g", entry->selectivities[i]);
			}
			appendStringInfoChar(&sel_buf, ']');
			if (entry->nclauses == 0)
				appendStringInfoString(&sel_buf, "::double precision[]");

			/*
			 * Build the relations array literal: ARRAY['users', 'orders', ...]
			 */
			appendStringInfoString(&rel_buf, "ARRAY[");
			for (i = 0; i < entry->nrels; i++)
			{
				if (i > 0)
					appendStringInfoString(&rel_buf, ", ");
				/* Escape single quotes in relation names */
				appendStringInfoChar(&rel_buf, '\'');
				for (const char *p = entry->relation_names[i]; *p; p++)
				{
					if (*p == '\'')
						appendStringInfoChar(&rel_buf, '\'');
					appendStringInfoChar(&rel_buf, *p);
				}
				appendStringInfoChar(&rel_buf, '\'');
			}
			appendStringInfoChar(&rel_buf, ']');
			if (entry->nrels == 0)
				appendStringInfoString(&rel_buf, "::text[]");

			/*
			 * Build clause_text: concatenate all pre-deparsed clauses with " AND ".
			 */
			{
				StringInfoData clause_buf;
				initStringInfo(&clause_buf);
				for (i = 0; i < entry->nclauses; i++)
				{
					if (i > 0)
						appendStringInfoString(&clause_buf, " AND ");
					if (entry->clause_texts[i])
						appendStringInfoString(&clause_buf, entry->clause_texts[i]);
				}

				/*
				 * Build the INSERT statement.
				 * Escape the clause text for SQL safety.
				 */
				appendStringInfo(&buf,
					"INSERT INTO aqo_node_context "
					"(query_hash, space_hash, node_type, join_type, "
					"clause_text, selectivities, estimated_cardinality, "
					"actual_cardinality, relations) VALUES ("
					INT64_FORMAT ", %d, '%s', '%s', ",
					(int64) entry->query_hash,
					entry->space_hash,
					nce_node_type_str(entry->node_type),
					nce_join_type_str(entry->jointype));

				/* Escape clause text properly using quote_literal_cstr */
				{
					char *escaped = quote_literal_cstr(clause_buf.data);
					appendStringInfoString(&buf, escaped);
					pfree(escaped);
				}

				appendStringInfo(&buf,
					", %s, %g, %g, %s)",
					sel_buf.data,
					entry->estimated_cardinality,
					entry->actual_cardinality,
					rel_buf.data);

				pfree(clause_buf.data);
			}

			ret = SPI_execute(buf.data, false, 0);
			if (ret != SPI_OK_INSERT)
				elog(WARNING, "[AQO NCE] INSERT failed for node (fss=%d): %d",
					 entry->space_hash, ret);

			pfree(buf.data);
			pfree(sel_buf.data);
			pfree(rel_buf.data);
		}

		PopActiveSnapshot();
		SPI_finish();
	}
	PG_CATCH();
	{
		/* Don't let NCE errors break normal query execution */
		EmitErrorReport();
		FlushErrorState();

		PG_TRY();
		{
			PopActiveSnapshot();
			SPI_finish();
		}
		PG_CATCH();
		{
			FlushErrorState();
		}
		PG_END_TRY();
	}
	PG_END_TRY();

	nce_in_flush = false;
	nce_reset_collected();
}

/*
 * nce_reset_collected
 *
 * Free all backend-local collected node entries and reset the list.
 */
void
nce_reset_collected(void)
{
	ListCell   *lc;

	foreach(lc, nce_collected_nodes)
	{
		NceCollectedNode *entry = (NceCollectedNode *) lfirst(lc);
		int		i;

		for (i = 0; i < entry->nclauses; i++)
		{
			if (entry->clause_texts[i])
				pfree(entry->clause_texts[i]);
		}
		for (i = 0; i < entry->nrels; i++)
		{
			if (entry->relation_names[i])
				pfree(entry->relation_names[i]);
		}
		pfree(entry);
	}

	list_free(nce_collected_nodes);
	nce_collected_nodes = NIL;
}

/*
 * SQL-callable function: aqo_node_context_reset()
 * Truncates the aqo_node_context table.
 */
PG_FUNCTION_INFO_V1(aqo_node_context_reset);

Datum
aqo_node_context_reset(PG_FUNCTION_ARGS)
{
	int ret;

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		elog(ERROR, "[AQO NCE] SPI_connect failed: %d", ret);

	ret = SPI_execute("TRUNCATE aqo_node_context", false, 0);
	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "[AQO NCE] TRUNCATE failed: %d", ret);

	SPI_finish();

	PG_RETURN_VOID();
}
