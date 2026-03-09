/*
 * node_context.h
 *		Node Context Extractor for AQO
 *
 * Extracts clause text, selectivities, cardinality estimates and other
 * context from PostgreSQL plan nodes during planning and execution.
 * Stores collected data into a real PostgreSQL table (aqo_node_context).
 *
 * Copyright (c) 2024, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/node_context.h
 */
#ifndef NODE_CONTEXT_H
#define NODE_CONTEXT_H

#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "path_utils.h"

/* Forward declaration — full definition in executor/execdesc.h */
struct QueryDesc;

/*
 * Maximum number of clauses we track per node and maximum text length
 * for a single clause's deparsed representation.
 */
#define NCE_MAX_CLAUSES		64
#define NCE_MAX_CLAUSE_LEN	2048
#define NCE_MAX_RELS		32

/*
 * Represents one collected plan node's context.
 * Allocated in TopMemoryContext so it survives across planning → execution.
 */
typedef struct NceCollectedNode
{
	uint64		query_hash;
	int			space_hash;			/* feature subspace hash (fss) */
	NodeTag		node_type;			/* plan node tag (T_SeqScan, T_HashJoin, etc.) */
	JoinType	jointype;

	/* Clause information */
	int			nclauses;
	char	   *clause_texts[NCE_MAX_CLAUSES]; /* deparsed SQL-like text */
	double		selectivities[NCE_MAX_CLAUSES]; /* per-clause selectivity */

	/* Cardinality */
	double		estimated_cardinality;	/* planner's estimate (or AQO prediction) */
	double		actual_cardinality;		/* filled during execution, -1 if unknown */

	/* Relation info */
	int			nrels;
	char	   *relation_names[NCE_MAX_RELS];

	/* Matching flag: set to true after actual_cardinality is filled */
	bool		matched;
} NceCollectedNode;

/* GUC variable */
extern bool aqo_nce_enabled;

/* Called from aqo_create_plan (path_utils.c) during planning */
extern void nce_collect_plan_node(PlannerInfo *root, Path *src, Plan *plan,
								  AQOPlanNode *aqo_node);

/* Called from learnOnPlanState (postprocessing.c) during execution */
extern void nce_update_actual_cardinality(int space_hash, NodeTag node_type,
										  double actual_rows);

/* Called from aqo_ExecutorEnd (postprocessing.c) to flush data to table */
extern void nce_flush_to_table(struct QueryDesc *queryDesc);

/* Reset backend-local collected list (called on error or cleanup) */
extern void nce_reset_collected(void);

#endif /* NODE_CONTEXT_H */
