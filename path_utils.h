#ifndef PATH_UTILS_H
#define PATH_UTILS_H

#include "nodes/extensible.h"
#include "nodes/pathnodes.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"

#define AQO_PLAN_NODE	"AQOPlanNode"

/*
 * information for adaptive query optimization
 */
typedef struct AQOPlanNode
{
	ExtensibleNode node;
	bool		had_path;
	List		*relids;
	List		*clauses;
	List		*selectivities;

	/* Grouping expressions from a target list. */
	List		*grouping_exprs;

	JoinType	jointype;
	double		parallel_divisor;
	bool		was_parametrized;

	/* For Adaptive optimization DEBUG purposes */
	int		fss;
	double	prediction;
} AQOPlanNode;


#define strtobool(x)  ((*(x) == 't') ? true : false)

#define nullable_string(token,length)  \
	((length) == 0 ? NULL : debackslash(token, length))

#define booltostr(x)  ((x) ? "true" : "false")

extern create_plan_hook_type prev_create_plan_hook;

/* Extracting path information utilities */
extern List *get_selectivities(PlannerInfo *root,
							   List *clauses,
							   int varRelid,
							   JoinType jointype,
							   SpecialJoinInfo *sjinfo);
extern List *get_list_of_relids(PlannerInfo *root, Relids relids);

extern List *get_path_clauses(Path *path,
							  PlannerInfo *root,
							  List **selectivities);

extern void aqo_create_plan_hook(PlannerInfo *root, Path *src, Plan **dest);
extern AQOPlanNode *get_aqo_plan_node(Plan *plan, bool create);
extern void RegisterAQOPlanNodeMethods(void);

extern create_upper_paths_hook_type prev_create_upper_paths_hook;
extern void aqo_store_upper_signature_hook(PlannerInfo *root,
										   UpperRelationKind stage,
										   RelOptInfo *input_rel,
										   RelOptInfo *output_rel,
										   void *extra);
extern List *aqo_get_clauses(PlannerInfo *root, List *restrictlist);

#endif /* PATH_UTILS_H */
