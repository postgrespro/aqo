#ifndef PROFILE_MEM_H
#define PROFILE_MEM_H

#include "postgres.h"

extern int 	aqo_profile_mem;
extern void set_profile_mem(int newval, void *extra);
extern void create_profile_mem_table(void);
extern void update_profile_mem_table(void);
extern Datum aqo_profile_mem_hash(PG_FUNCTION_ARGS);

#endif /* PROFILE_MEM_H */