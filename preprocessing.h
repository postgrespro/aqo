#ifndef __PREPROCESSING_H__
#define __PREPROCESSING_H__

#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
extern PlannedStmt *aqo_planner(Query *parse,
								const char *query_string,
								int cursorOptions,
								ParamListInfo boundParams);
extern void disable_aqo_for_query(void);

#endif /* __PREPROCESSING_H__ */
