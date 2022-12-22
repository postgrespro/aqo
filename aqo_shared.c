/*
 *
 */

#include "postgres.h"

#include "lib/dshash.h"
#include "miscadmin.h"
#include "storage/shmem.h"

#include "aqo_shared.h"
#include "storage.h"


typedef struct
{
	int		magic;
	uint32	total_size;
	uint32	delta;
} dsm_seg_hdr;

#define free_space(hdr) (uint32) (temp_storage_size - sizeof(dsm_seg_hdr) - hdr->delta)
#define addr(delta)	((char *) dsm_segment_address(seg) + sizeof(dsm_seg_hdr) + delta)

shmem_startup_hook_type prev_shmem_startup_hook = NULL;
AQOSharedState *aqo_state = NULL;
HTAB *fss_htab = NULL;
static int aqo_htab_max_items = 1000;
int fs_max_items = 10000; /* Max number of different feature spaces in ML model */
int fss_max_items = 100000; /* Max number of different feature subspaces in ML model */
static uint32 temp_storage_size = 1024 * 1024 * 10; /* Storage size, in bytes */
static dsm_segment *seg = NULL;


static void aqo_detach_shmem(int code, Datum arg);
static void on_shmem_shutdown(int code, Datum arg);


void *
get_dsm_all(uint32 *size)
{
	dsm_seg_hdr	*hdr;

	Assert(LWLockHeldByMeInMode(&aqo_state->lock, LW_EXCLUSIVE));

	if (aqo_state->dsm_handler == DSM_HANDLE_INVALID)
	{
		/* Fast path. No any cached data exists. */
		*size = 0;
		return NULL;
	}

	if (!seg)
	{
		/* if segment exists we should connect to */
		seg = dsm_attach(aqo_state->dsm_handler);
		Assert(seg);
		dsm_pin_mapping(seg);
		before_shmem_exit(aqo_detach_shmem, (Datum) &aqo_state->dsm_handler);
	}

	hdr = (dsm_seg_hdr	*) dsm_segment_address(seg);
	*size = hdr->delta;
	return (char *) hdr + sizeof(dsm_seg_hdr);
}

/*
 * Cleanup of DSM cache: set header into default state and zero the memory block.
 * This operation can be coupled with the cache dump, so we do it under an external
 * hold of the lock.
 */
void
reset_dsm_cache(void)
{
	dsm_seg_hdr	*hdr;
	char		*start;

	Assert(LWLockHeldByMeInMode(&aqo_state->lock, LW_EXCLUSIVE));

	if (aqo_state->dsm_handler == DSM_HANDLE_INVALID || !seg)
		/* Fast path. No any cached data exists. */
		return;

	hdr = (dsm_seg_hdr *) dsm_segment_address(seg);
	start = (char *) hdr + sizeof(dsm_seg_hdr);

	/* Reset the cache */
	memset(start, 0, hdr->delta);

	hdr->delta = 0;
	hdr->total_size = temp_storage_size - sizeof(dsm_seg_hdr);
}

char *
get_cache_address(void)
{
	dsm_seg_hdr *hdr;

	Assert(LWLockHeldByMeInMode(&aqo_state->lock, LW_EXCLUSIVE) ||
		   LWLockHeldByMeInMode(&aqo_state->lock, LW_SHARED));

	if (aqo_state->dsm_handler != DSM_HANDLE_INVALID)
	{
		if (!seg)
		{
			/* Another process created the segment yet. Just attach to. */
			seg = dsm_attach(aqo_state->dsm_handler);
			dsm_pin_mapping(seg);
			before_shmem_exit(aqo_detach_shmem, (Datum) &aqo_state->dsm_handler);
		}

		hdr = (dsm_seg_hdr	*) dsm_segment_address(seg);
	}
	else
	{
		/*
		 * First request for DSM cache in this instance.
		 * Create the DSM segment. Pin it to live up to instance shutdown.
		 * Don't forget to detach DSM segment before an exit.
		 */
		seg = dsm_create(temp_storage_size, 0);
		dsm_pin_mapping(seg);
		dsm_pin_segment(seg);
		aqo_state->dsm_handler = dsm_segment_handle(seg);
		before_shmem_exit(aqo_detach_shmem, (Datum) &aqo_state->dsm_handler);

		hdr = (dsm_seg_hdr *) dsm_segment_address(seg);
		hdr->magic = AQO_SHARED_MAGIC;
		hdr->delta = 0;
		hdr->total_size = temp_storage_size - sizeof(dsm_seg_hdr);
	}

	Assert(seg);
	Assert(hdr->magic == AQO_SHARED_MAGIC && hdr->total_size > 0);

	return (char *) hdr + sizeof(dsm_seg_hdr);
}

uint32
get_dsm_cache_pos(uint32 size)
{
	dsm_seg_hdr	   *hdr;
	uint32			pos;

	Assert(LWLockHeldByMeInMode(&aqo_state->lock, LW_EXCLUSIVE) ||
		   LWLockHeldByMeInMode(&aqo_state->lock, LW_SHARED));

	(void) get_cache_address();
	hdr = (dsm_seg_hdr	*) dsm_segment_address(seg);

	if (free_space(hdr) < size || size == 0)
		elog(ERROR,
			 "DSM cache can't allcoate a mem block. Required: %u, free: %u",
			 size, free_space(hdr));

	pos = hdr->delta;
	hdr->delta += size;
	Assert(free_space(hdr) >= 0);
	return pos;
}

static void
aqo_detach_shmem(int code, Datum arg)
{
	if (seg != NULL)
		dsm_detach(seg);
	seg = NULL;
}

void
aqo_init_shmem(void)
{
	bool		found;
	HASHCTL		info;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	aqo_state = NULL;
	fss_htab = NULL;
	stat_htab = NULL;
	qtexts_htab = NULL;
	data_htab = NULL;
	queries_htab = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	aqo_state = ShmemInitStruct("AQO", sizeof(AQOSharedState), &found);
	if (!found)
	{
		/* First time through ... */

		aqo_state->dsm_handler = DSM_HANDLE_INVALID;
		aqo_state->qtexts_dsa_handler = DSM_HANDLE_INVALID;
		aqo_state->data_dsa_handler = DSM_HANDLE_INVALID;

		aqo_state->qtext_trancheid = LWLockNewTrancheId();

		aqo_state->qtexts_changed = false;
		aqo_state->stat_changed = false;
		aqo_state->data_changed = false;
		aqo_state->queries_changed = false;

		LWLockInitialize(&aqo_state->lock, LWLockNewTrancheId());
		LWLockInitialize(&aqo_state->stat_lock, LWLockNewTrancheId());
		LWLockInitialize(&aqo_state->qtexts_lock, LWLockNewTrancheId());
		LWLockInitialize(&aqo_state->data_lock, LWLockNewTrancheId());
		LWLockInitialize(&aqo_state->queries_lock, LWLockNewTrancheId());
	}

	info.keysize = sizeof(htab_key);
	info.entrysize = sizeof(htab_entry);
	fss_htab = ShmemInitHash("AQO hash",
							  aqo_htab_max_items, aqo_htab_max_items,
							  &info,
							  HASH_ELEM | HASH_BLOBS);

	info.keysize = sizeof(((StatEntry *) 0)->queryid);
	info.entrysize = sizeof(StatEntry);
	stat_htab = ShmemInitHash("AQO Stat HTAB", fs_max_items, fs_max_items,
							  &info, HASH_ELEM | HASH_BLOBS);

	/* Init shared memory table for query texts */
	info.keysize = sizeof(((QueryTextEntry *) 0)->queryid);
	info.entrysize = sizeof(QueryTextEntry);
	qtexts_htab = ShmemInitHash("AQO Query Texts HTAB", fs_max_items, fs_max_items,
								&info, HASH_ELEM | HASH_BLOBS);

	/* Shared memory hash table for the data */
	info.keysize = sizeof(data_key);
	info.entrysize = sizeof(DataEntry);
	data_htab = ShmemInitHash("AQO Data HTAB", fss_max_items, fss_max_items,
							  &info, HASH_ELEM | HASH_BLOBS);

	/* Shared memory hash table for queries */
	info.keysize = sizeof(((QueriesEntry *) 0)->queryid);
	info.entrysize = sizeof(QueriesEntry);
	queries_htab = ShmemInitHash("AQO Queries HTAB", fs_max_items, fs_max_items,
								 &info, HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
	LWLockRegisterTranche(aqo_state->lock.tranche, "AQO");
	LWLockRegisterTranche(aqo_state->stat_lock.tranche, "AQO Stat Lock Tranche");
	LWLockRegisterTranche(aqo_state->qtexts_lock.tranche, "AQO QTexts Lock Tranche");
	LWLockRegisterTranche(aqo_state->qtext_trancheid, "AQO Query Texts Tranche");
	LWLockRegisterTranche(aqo_state->data_lock.tranche, "AQO Data Lock Tranche");
	LWLockRegisterTranche(aqo_state->queries_lock.tranche, "AQO Queries Lock Tranche");

	if (!IsUnderPostmaster && !found)
	{
		before_shmem_exit(on_shmem_shutdown, (Datum) 0);

		 /* Doesn't use DSA, so can be loaded in postmaster */
		aqo_stat_load();
		aqo_queries_load();
	}
}

/*
 * Main idea here is to store all ML data in temp files on postmaster shutdown.
 */
static void
on_shmem_shutdown(int code, Datum arg)
{
	Assert(!IsUnderPostmaster);

	/*
	 * Save ML data to a permanent storage. Do it on postmaster shutdown only
	 * to save time. We can't do so for query_texts and aqo_data because of DSM
	 * limits.
	 */
	aqo_stat_flush();
	aqo_queries_flush();
	return;
}

Size
aqo_memsize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(AQOSharedState));
	size = add_size(size, hash_estimate_size(aqo_htab_max_items, sizeof(htab_entry)));
	size = add_size(size, hash_estimate_size(fs_max_items, sizeof(AQOSharedState)));
	size = add_size(size, hash_estimate_size(fs_max_items, sizeof(StatEntry)));
	size = add_size(size, hash_estimate_size(fs_max_items, sizeof(QueryTextEntry)));
	size = add_size(size, hash_estimate_size(fss_max_items, sizeof(DataEntry)));
	size = add_size(size, hash_estimate_size(fs_max_items, sizeof(QueriesEntry)));

	return size;
}
