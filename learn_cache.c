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
	double	   *targets;
	List	   *relids;
} htab_entry;

static HTAB *fss_htab = NULL;
MemoryContext LearnCacheMemoryContext = NULL;

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

	fss_htab = hash_create("Remote Con hash", 32, &ctl, HASH_ELEM | HASH_BLOBS);
}

bool
lc_update_fss(uint64 fs, int fss, int nrows, int ncols,
			  double **matrix, double *targets, List *relids)
{
	htab_key		key = {fs, fss};
	htab_entry	   *entry;
	bool			found;
	int				i;
	MemoryContext	memctx = MemoryContextSwitchTo(LearnCacheMemoryContext);

	Assert(fss_htab);

	entry = (htab_entry *) hash_search(fss_htab, &key, HASH_ENTER, &found);
	if (found)
	{
		/* Clear previous version of the cached data. */
		for (i = 0; i < entry->nrows; ++i)
			pfree(entry->matrix[i]);
		pfree(entry->targets);
		list_free(entry->relids);
	}

	entry->nrows = nrows;
	entry->ncols = ncols;
	for (i = 0; i < entry->nrows; ++i)
	{
		entry->matrix[i] = palloc(sizeof(double) * ncols);
		memcpy(entry->matrix[i], matrix[i], sizeof(double) * ncols);
	}
	entry->targets = palloc(sizeof(double) * nrows);
	memcpy(entry->targets, targets, sizeof(double) * nrows);
	entry->relids = list_copy(relids);

	MemoryContextSwitchTo(memctx);
	return true;
}

bool
lc_has_fss(uint64 fs, int fss)
{
	htab_key	key = {fs, fss};
	bool		found;

	Assert(fss_htab);

	(void) hash_search(fss_htab, &key, HASH_FIND, &found);
	if (!found)
		return false;
	return true;
}

bool
lc_load_fss(uint64 fs, int fss, int ncols, double **matrix,
			double *targets, int *nrows, List **relids)
{
	htab_key	key = {fs, fss};
	htab_entry	*entry;
	bool		found;
	int			i;

	Assert(fss_htab);

	entry = (htab_entry *) hash_search(fss_htab, &key, HASH_FIND, &found);
	if (!found)
		return false;

	*nrows = entry->nrows;
	Assert(entry->ncols == ncols);
	for (i = 0; i < entry->nrows; ++i)
		memcpy(matrix[i], entry->matrix[i], sizeof(double) * ncols);
	memcpy(targets, entry->targets, sizeof(double) *  entry->nrows);
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

	Assert(fss_htab);

	entry = (htab_entry *) hash_search(fss_htab, &key, HASH_FIND, &found);
	if (!found)
		return;

	for (i = 0; i < entry->nrows; ++i)
		pfree(entry->matrix[i]);
	pfree(entry->targets);
	hash_search(fss_htab, &key, HASH_REMOVE, NULL);
}
