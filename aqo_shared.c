/*
 *
 */

#include "postgres.h"

#include "lib/dshash.h"
#include "miscadmin.h"
#include "storage/shmem.h"

#include "aqo_shared.h"
#include "storage.h"


shmem_startup_hook_type prev_shmem_startup_hook = NULL;
AQOSharedState *aqo_state = NULL;
int fs_max_items = 10000; /* Max number of different feature spaces in ML model */
int fss_max_items = 100000; /* Max number of different feature subspaces in ML model */

static void on_shmem_shutdown(int code, Datum arg);

void
aqo_init_shmem(void)
{
	bool		found;
	HASHCTL		info;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	aqo_state = NULL;
	stat_htab = NULL;
	qtexts_htab = NULL;
	data_htab = NULL;
	queries_htab = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	aqo_state = ShmemInitStruct("AQO", sizeof(AQOSharedState), &found);
	if (!found)
	{
		/* First time through ... */

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
	size = add_size(size, hash_estimate_size(fs_max_items, sizeof(AQOSharedState)));
	size = add_size(size, hash_estimate_size(fs_max_items, sizeof(StatEntry)));
	size = add_size(size, hash_estimate_size(fs_max_items, sizeof(QueryTextEntry)));
	size = add_size(size, hash_estimate_size(fss_max_items, sizeof(DataEntry)));
	size = add_size(size, hash_estimate_size(fs_max_items, sizeof(QueriesEntry)));

	return size;
}
