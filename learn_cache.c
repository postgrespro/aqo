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
#include "miscadmin.h"

#include "aqo.h"
#include "learn_cache.h"

typedef struct
{
	/* XXX we assume this struct contains no padding bytes */
	uint64 fs;
	int64 fss;
} htab_key;

typedef struct
{
	htab_key	key;

	/* Store ML data "AS IS". */
	int			nrows;
	int			ncols;
	double	   *matrix[aqo_K];
	double		targets[aqo_K];
	double		rfactors[aqo_K];
	List	   *relids;
} htab_entry;

static HTAB *fss_htab = NULL;
MemoryContext LearnCacheMemoryContext = NULL;

bool aqo_learn_statement_timeout = false;

void
lc_init(void)
{
	HASHCTL ctl;

	Assert(!LearnCacheMemoryContext);
	LearnCacheMemoryContext = AllocSetContextCreate(TopMemoryContext,
													"lcache context",
													ALLOCSET_DEFAULT_SIZES);

	ctl.keysize = sizeof(htab_key);
	ctl.entrysize = sizeof(htab_entry);
	ctl.hcxt = LearnCacheMemoryContext;

	fss_htab = hash_create("ML AQO cache", 256, &ctl, HASH_ELEM | HASH_BLOBS);
}

bool
lc_update_fss(uint64 fs, int fss, OkNNrdata *data, List *relids)
{
	htab_key		key = {fs, fss};
	htab_entry	   *entry;
	bool			found;
	int				i;
	MemoryContext	memctx = MemoryContextSwitchTo(LearnCacheMemoryContext);

	Assert(fss_htab && aqo_learn_statement_timeout);

	entry = (htab_entry *) hash_search(fss_htab, &key, HASH_ENTER, &found);
	if (found)
	{
		/* Clear previous version of the cached data. */
		for (i = 0; i < entry->nrows; ++i)
			pfree(entry->matrix[i]);
		list_free(entry->relids);
	}

	entry->nrows = data->rows;
	entry->ncols = data->cols;
	for (i = 0; i < entry->nrows; ++i)
	{
		entry->matrix[i] = palloc(sizeof(double) * data->cols);
		memcpy(entry->matrix[i], data->matrix[i], sizeof(double) * data->cols);
	}

	memcpy(entry->targets, data->targets, sizeof(double) * data->rows);
	memcpy(entry->rfactors, data->rfactors, sizeof(double) * data->rows);
	entry->relids = list_copy(relids);

	MemoryContextSwitchTo(memctx);
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

	(void) hash_search(fss_htab, &key, HASH_FIND, &found);
	return found;
}

/*
 * Load ML data from a memory cache, not from a table.
 * XXX That to do with learning tails, living in the cache?
 */
bool
lc_load_fss(uint64 fs, int fss, OkNNrdata *data, List **relids)
{
	htab_key	key = {fs, fss};
	htab_entry	*entry;
	bool		found;
	int			i;

	Assert(fss_htab && aqo_learn_statement_timeout);

	entry = (htab_entry *) hash_search(fss_htab, &key, HASH_FIND, &found);
	if (!found)
		return false;

	if (aqo_show_details)
		elog(NOTICE, "[AQO] Load ML data for fs %lu, fss %d from the cache",
			 fs, fss);

	data->rows = entry->nrows;
	Assert(entry->ncols == data->cols);
	for (i = 0; i < entry->nrows; ++i)
		memcpy(data->matrix[i], entry->matrix[i], sizeof(double) * data->cols);
	memcpy(data->targets, entry->targets, sizeof(double) * entry->nrows);
	memcpy(data->rfactors, entry->rfactors, sizeof(double) * entry->nrows);
	if (relids)
		*relids = list_copy(entry->relids);
	return true;
}

/*
 * Remove record from fss cache. Should be done at learning stage of successfully
 * finished query execution.
*/
void
lc_remove_fss(uint64 fs, int fss)
{
	htab_key	key = {fs, fss};
	htab_entry *entry;
	bool		found;
	int			i;

	if (!aqo_learn_statement_timeout)
		return;

	Assert(fss_htab);

	entry = (htab_entry *) hash_search(fss_htab, &key, HASH_FIND, &found);
	if (!found)
		return;

	for (i = 0; i < entry->nrows; ++i)
		pfree(entry->matrix[i]);

	hash_search(fss_htab, &key, HASH_REMOVE, NULL);
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

	if (!fss_htab || !IsUnderPostmaster)
		return;

	/* Remove all entries, reset memory context. */

	elog(DEBUG5, "[AQO] Cleanup local cache of ML data.");

	/* Remove all frozen plans from a plancache. */
	hash_seq_init(&status, fss_htab);
	while ((entry = (htab_entry *) hash_seq_search(&status)) != NULL)
	{
		if (!hash_search(fss_htab, (void *) &entry->key, HASH_REMOVE, NULL))
			elog(ERROR, "[AQO] The local ML cache is corrupted.");
	}

	MemoryContextReset(LearnCacheMemoryContext);
}