/*
 *******************************************************************************
 *
 *
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/learn_cache.c
 *
 */

#include "postgres.h"
#include "access/parallel.h" /* Just for IsParallelWorker() */
#include "miscadmin.h"

#include "aqo.h"
#include "aqo_shared.h"
#include "learn_cache.h"
#include "storage.h"


typedef struct
{
	int			magic;
	htab_key	key;
	int			rows;
	int			cols;
	int			nrelids;

	/*
	 * Links to variable data:
	 * double	   *matrix[aqo_K];
	 * double	   *targets;
	 * double	   *rfactors;
	 * int		   *relids;
	 */
} dsm_block_hdr;


bool aqo_learn_statement_timeout = false;

static uint32 init_with_dsm(OkNNrdata *data, dsm_block_hdr *hdr, List **relids);


/* Calculate, how many data we need to store an ML record. */
static uint32
calculate_size(int cols, List *reloids)
{
	uint32		size = sizeof(dsm_block_hdr); /* header's size */

	size += sizeof(double) * cols * aqo_K; /* matrix */
	size += 2 * sizeof(double) * aqo_K; /* targets, rfactors */

	/* Calculate memory size needed to store relation names */
	size += list_length(reloids) * sizeof(Oid);
	return size;
}

bool
lc_update_fss(uint64 fs, int fss, OkNNrdata *data, List *reloids)
{
	htab_key		key = {fs, fss};
	htab_entry	   *entry;
	dsm_block_hdr  *hdr;
	char		   *ptr;
	bool			found;
	int				i;
	ListCell	   *lc;
	uint32			size;

	Assert(fss_htab && aqo_learn_statement_timeout);

	size = calculate_size(data->cols, reloids);
	LWLockAcquire(&aqo_state->lock, LW_EXCLUSIVE);

	entry = (htab_entry *) hash_search(fss_htab, &key, HASH_ENTER, &found);
	if (found)
	{
		hdr = (dsm_block_hdr *) (get_cache_address() + entry->hdr_off);

		Assert(hdr->magic == AQO_SHARED_MAGIC);
		Assert(hdr->key.fs == fs && hdr->key.fss == fss);

		if (data->cols != hdr->cols || list_length(reloids) != hdr->nrelids)
		{
			/*
			 * Collision found: the same {fs,fss}, but something different.
			 * For simplicity - just don't update.
			 */
			elog(DEBUG5, "[AQO]: A collision found in the temporary storage.");
			LWLockRelease(&aqo_state->lock);
			return false;
		}
	}
	else
	{
		/* Get new block of DSM */
		entry->hdr_off = get_dsm_cache_pos(size);
		hdr = (dsm_block_hdr *) (get_cache_address() + entry->hdr_off);

		/* These fields shouldn't change */
		hdr->magic = AQO_SHARED_MAGIC;
		hdr->key.fs = fs;
		hdr->key.fss = fss;
		hdr->cols = data->cols;
		hdr->nrelids = list_length(reloids);
	}

	hdr->rows = data->rows;
	ptr = (char *) hdr + sizeof(dsm_block_hdr); /* start point of variable data */

	/* copy the matrix into DSM storage */

	if (hdr->cols > 0)
	{
		for (i = 0; i < aqo_K; ++i)
		{
			if (i >= hdr->rows)
				break;

			if (!ptr || !data->matrix[i])
				elog(PANIC, "Something disruptive have happened! %d, %d (%d %d)", i, hdr->rows, found, hdr->cols);
			memcpy(ptr, data->matrix[i], sizeof(double) * hdr->cols);
			ptr += sizeof(double) * data->cols;
		}
	}

	/*
	 * Kludge code. But we should rewrite this code because now all knowledge
	 * base lives in non-transactional shared memory.
	 */
	ptr = (char *) hdr + sizeof(dsm_block_hdr) + (sizeof(double) * data->cols * aqo_K);

	/* copy targets into DSM storage */
	memcpy(ptr, data->targets, sizeof(double) * hdr->rows);
	ptr += sizeof(double) * aqo_K;

	/* copy rfactors into DSM storage */
	memcpy(ptr, data->rfactors, sizeof(double) * hdr->rows);
	ptr += sizeof(double) * aqo_K;

	/* store list of relations */
	foreach(lc, reloids)
	{
		Oid reloid = lfirst_oid(lc);

		memcpy(ptr, &reloid, sizeof(Oid));
		ptr += sizeof(Oid);
	}

	/* Check the invariant */
	Assert((uint32)(ptr - (char *) hdr) == size);

	elog(DEBUG5, "DSM entry: %s, targets: %d.",
		 found ? "Reused" : "New entry", hdr->rows);
	LWLockRelease(&aqo_state->lock);
	return true;
}

bool
lc_has_fss(uint64 fs, int fss)
{
	htab_key	key = {fs, fss};
	bool		found;

	if (!aqo_learn_statement_timeout)
		return false;

	Assert(fss_htab);

	LWLockAcquire(&aqo_state->lock, LW_SHARED);
	(void) hash_search(fss_htab, &key, HASH_FIND, &found);
	LWLockRelease(&aqo_state->lock);

	return found;
}

/*
 * Load ML data from a memory cache, not from a table.
 */
bool
lc_load_fss(uint64 fs, int fss, OkNNrdata *data, List **reloids)
{
	htab_key		key = {fs, fss};
	htab_entry	   *entry;
	bool			found;
	dsm_block_hdr  *hdr;

	Assert(fss_htab && aqo_learn_statement_timeout);

	if (aqo_show_details)
		elog(NOTICE, "[AQO] Load ML data for fs "UINT64_FORMAT", fss %d from the cache",
			 fs, fss);

	LWLockAcquire(&aqo_state->lock, LW_SHARED);
	entry = (htab_entry *) hash_search(fss_htab, &key, HASH_FIND, &found);
	if (!found)
	{
		LWLockRelease(&aqo_state->lock);
		return false;
	}

	hdr = (dsm_block_hdr *) (get_cache_address() + entry->hdr_off);
	Assert(hdr->magic == AQO_SHARED_MAGIC);
	Assert(hdr->key.fs == fs && hdr->key.fss == fss);

	/* XXX */
	if (hdr->cols != data->cols)
	{
		LWLockRelease(&aqo_state->lock);
		return false;
	}

	init_with_dsm(data, hdr, reloids);
	LWLockRelease(&aqo_state->lock);
	return true;
}

static uint32
init_with_dsm(OkNNrdata *data, dsm_block_hdr *hdr, List **reloids)
{
	int		i;
	char   *ptr = (char *) hdr + sizeof(dsm_block_hdr);

	Assert(LWLockHeldByMeInMode(&aqo_state->lock, LW_EXCLUSIVE) ||
		   LWLockHeldByMeInMode(&aqo_state->lock, LW_SHARED));
	Assert(hdr->magic == AQO_SHARED_MAGIC);
	Assert(hdr && ptr && hdr->rows > 0);

	data->rows = hdr->rows;
	data->cols = hdr->cols;

	if (data->cols > 0)
	{
		for (i = 0; i < aqo_K; ++i)
		{
			if (i < data->rows)
			{
				data->matrix[i] = palloc(sizeof(double) * data->cols);
				memcpy(data->matrix[i], ptr, sizeof(double) * data->cols);
			}
			ptr += sizeof(double) * data->cols;
		}
	}

	/*
	 * Kludge code. But we should rewrite this code because now all knowledge
	 * base lives in non-transactional shared memory.
	 */
	ptr = (char *) hdr + sizeof(dsm_block_hdr) + (sizeof(double) * data->cols * aqo_K);

	memcpy(data->targets, ptr, sizeof(double) * hdr->rows);
	ptr += sizeof(double) * aqo_K;
	memcpy(data->rfactors, ptr, sizeof(double) * hdr->rows);
	ptr += sizeof(double) * aqo_K;

	if (reloids)
	{
		*reloids = NIL;
		for (i = 0; i < hdr->nrelids; i++)
		{
			*reloids = lappend_oid(*reloids, *(Oid *)(ptr));
			ptr += sizeof(Oid);
		}
		return calculate_size(hdr->cols, *reloids);
	}

	/* It is just a read operation. No any interest in size calculation. */
	return 0;
}

void
lc_flush_data(void)
{
	char   *ptr;
	uint32	size;

	if (aqo_state->dsm_handler == DSM_HANDLE_INVALID)
		/* Fast path. No any cached data exists. */
		return;

	LWLockAcquire(&aqo_state->lock, LW_EXCLUSIVE);
	ptr = get_dsm_all(&size);

	/* Iterate through records and store them into the aqo_data table */
	while (size > 0)
	{
		dsm_block_hdr  *hdr = (dsm_block_hdr *) ptr;
		OkNNrdata		data;
		List		   *reloids = NIL;
		uint32			delta = 0;

		delta = init_with_dsm(&data, hdr, &reloids);
		Assert(delta > 0);
		ptr += delta;
		size -= delta;
		aqo_data_store(hdr->key.fs, hdr->key.fss, &data, reloids);

		if (!hash_search(fss_htab, (void *) &hdr->key, HASH_REMOVE, NULL))
			elog(PANIC, "[AQO] Flush: local ML cache is corrupted.");
	}

	reset_dsm_cache();
	LWLockRelease(&aqo_state->lock);
}

/*
 * Main purpose of this hook is to cleanup a backend cache in some way to prevent
 * memory leaks - in large queries we could have many unused fss nodes.
 */
void
lc_assign_hook(bool newval, void *extra)
{
	HASH_SEQ_STATUS		status;
	htab_entry		   *entry;

	if (!fss_htab || !IsUnderPostmaster || IsParallelWorker())
		/* Clean this shared cache only in main backend process. */
		return;

	/* Remove all entries, reset memory context. */

	elog(DEBUG5, "[AQO] Cleanup local cache of ML data.");

	/* Remove all entries in the shared hash table. */
	LWLockAcquire(&aqo_state->lock, LW_EXCLUSIVE);
	hash_seq_init(&status, fss_htab);
	while ((entry = (htab_entry *) hash_seq_search(&status)) != NULL)
	{
		if (!hash_search(fss_htab, (void *) &entry->key, HASH_REMOVE, NULL))
			elog(PANIC, "[AQO] The local ML cache is corrupted.");
	}

	/* Now, clean additional DSM block */
	reset_dsm_cache();

	LWLockRelease(&aqo_state->lock);
}
