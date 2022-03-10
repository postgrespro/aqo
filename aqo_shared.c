/*
 *
 */

#include "postgres.h"

#include "storage/shmem.h"

#include "aqo_shared.h"

shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static AQOSharedState *aqo_state = NULL;
unsigned long temp_storage_size = 1024 * 1024; /* Storage size, in bytes */
void *temp_storage = NULL;

static void
attach_dsm_segment(void)
{
	dsm_segment *seg;

	LWLockAcquire(&aqo_state->lock, LW_EXCLUSIVE);

	if (aqo_state->dsm_handler != DSM_HANDLE_INVALID)
	{
		seg = dsm_attach(aqo_state->dsm_handler);
	}
	else
	{
		seg = dsm_create(temp_storage_size, 0);
		aqo_state->dsm_handler = dsm_segment_handle(seg);
	}

	temp_storage = dsm_segment_address(seg);
	LWLockRelease(&aqo_state->lock);
}

static void
aqo_detach_shmem(int code, Datum arg)
{
	dsm_handle handler = *(dsm_handle *) arg;
	dsm_detach(dsm_find_mapping(handler));
}

void
aqo_init_shmem(void)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	aqo_state = ShmemInitStruct("aqo", sizeof(AQOSharedState), &found);
	if (!found)
	{
		/* First time through ... */
		LWLockInitialize(&aqo_state->lock, LWLockNewTrancheId());
		aqo_state->dsm_handler = DSM_HANDLE_INVALID;
	}
	LWLockRelease(AddinShmemInitLock);

	LWLockRegisterTranche(aqo_state->lock.tranche, "aqo");
	on_shmem_exit(aqo_detach_shmem, (Datum) &aqo_state->dsm_handler);
}
