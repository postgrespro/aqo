#ifndef AQO_SHARED_H
#define AQO_SHARED_H


#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"


typedef struct AQOSharedState
{
	LWLock		lock;			/* mutual exclusion */
	dsm_handle	dsm_handler;
} AQOSharedState;


extern shmem_startup_hook_type prev_shmem_startup_hook;


extern void aqo_init_shmem(void);

#endif /* AQO_SHARED_H */
