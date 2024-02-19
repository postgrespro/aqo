#ifndef PATH_UTILS_H
#define PATH_UTILS_H

#include "nodes/extensible.h"
#include "nodes/pathnodes.h"
#include "optimizer/planner.h"

#define AQO_PLAN_NODE	"AQOPlanNode"
#define AQO_CONST_NODE	"AQOConstNode"

extern List *aqo_eclass_collector;

/*
 * Find and sort out relations that used in the query:
 * Use oids of relations to store dependency of ML row on a set of tables.
 * Use oids of temporary tables to get access to these structure for preparing
 * a kind of signature.
 */
typedef struct
{
	List *hrels; /* oids of persistent relations */
	List *signatures; /* list of hashes: on qualified name of a persistent
						 * table or on a table structure for temp table */
} RelSortOut;

/*
 * Fields of the RestrictInfo needed in the AQOPlanNode
 */
typedef struct AQOClause
{
	/* the represented clause of WHERE or JOIN */
	Expr	   *clause;
	/* selectivity for "normal" (JOIN_INNER) semantics; -1 if not yet set */
	Selectivity norm_selec;
	/* selectivity for outer join semantics; -1 if not yet set */
	Selectivity outer_selec;

	/* Serial number of EquivalenceClass containing lefthand */
	int			left_ec;
	/* Serial number of EquivalenceClass containing righthand */
	int			right_ec;
	/* Quick check for equivalence class */
	bool		is_eq_clause;

	EquivalenceClass *ec;
} AQOClause;

/*
 * information for adaptive query optimization
 */
typedef struct AQOPlanNode
{
	ExtensibleNode	node;
	bool			had_path;
	RelSortOut	 	rels;
	List		   *clauses;
	List		   *selectivities;

	/* Grouping expressions from a target list. */
	List		*grouping_exprs;

	JoinType	jointype;
	double		parallel_divisor;
	bool		was_parametrized;

	/* For Adaptive optimization DEBUG purposes */
	int		fss;
	double	prediction;
} AQOPlanNode;

/*
 * The type of a node that is replaced by AQOConstNode.
 */
typedef enum AQOConstType
{
	AQO_NODE_EXPR = 0,
	AQO_NODE_SUBPLAN
} AQOConstType;

/*
 * A custom node that is used to calcucate a fss instead of regular node,
 * such as SubPlan or Expr.
 */
typedef struct AQOConstNode
{
	ExtensibleNode	node;
	AQOConstType	type;	/* The type of the replaced node */
	int				fss;	/* The fss of the replaced node */
} AQOConstNode;

#define strtobool(x)  ((*(x) == 't') ? true : false)

#define nullable_string(token,length)  \
	((length) == 0 ? NULL : debackslash(token, length))

#define booltostr(x)  ((x) ? "true" : "false")

/* Extracting path information utilities */
extern List *get_selectivities(PlannerInfo *root,
							   List *clauses,
							   int varRelid,
							   JoinType jointype,
							   SpecialJoinInfo *sjinfo);
extern void get_list_of_relids(PlannerInfo *root, Relids relids,
							   RelSortOut *rels);

extern List *get_path_clauses(Path *path,
							  PlannerInfo *root,
							  List **selectivities);

extern AQOConstNode *create_aqo_const_node(AQOConstType type, int fss);

extern AQOPlanNode *get_aqo_plan_node(Plan *plan, bool create);
extern void RegisterAQOPlanNodeMethods(void);

extern List *aqo_get_clauses(PlannerInfo *root, List *restrictlist);

void aqo_path_utils_init(void);

#endif /* PATH_UTILS_H */
