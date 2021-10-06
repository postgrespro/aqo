#ifndef PROFILE_MEM_H
#define PROFILE_MEM_H

#include "storage/ipc.h"
#include "utils/guc.h"

extern PGDLLIMPORT int 	aqo_profile_classes;
extern PGDLLIMPORT bool aqo_profile_enable;
extern PGDLLIMPORT shmem_startup_hook_type prev_shmem_startup_hook;

extern long profile_clear_hash_table(void);
extern bool check_aqo_profile_enable(bool *newval, void **extra, GucSource source);
extern void update_profile_mem_table(double total_time);

extern void profile_init(void);
extern void profile_shmem_startup(void);

#endif /* PROFILE_MEM_H */
