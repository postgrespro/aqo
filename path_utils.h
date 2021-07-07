#ifndef PATH_UTILS_H
#define PATH_UTILS_H

#include "postgres.h"

#include "nodes/pathnodes.h"

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

#endif /* PATH_UTILS_H */
