#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "aqo.h"
#include "profile_mem.h"


int 	aqo_profile_classes;
bool	aqo_profile_enable;
bool	out_of_memory = false;
static HTAB   *profile_mem_queries = NULL;

typedef struct ProfileMemEntry
{
	uint64 key;
	double time;
	unsigned int counter;
} ProfileMemEntry;

PG_FUNCTION_INFO_V1(aqo_show_classes);
PG_FUNCTION_INFO_V1(aqo_clear_classes);

shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/*
 * Check a state of shared memory allcated for classes buffer.
 * Warn user, if he want to enable profiling without shared memory at all.
 */
bool
check_aqo_profile_enable(bool *newval, void **extra, GucSource source)
{
	if (*newval == true && aqo_profile_classes <= 0)
	{
		elog(WARNING, "Before enabling profiling, set the value of the aqo_profile_classes GUC to allocate a buffer storage.");
		return false;
	}

	/* If it isn't startup process, we should check a shared memory existence. */
	Assert(!IsUnderPostmaster || !(*newval == true && !profile_mem_queries));

	return true;
}

/*
 * Returns query classes.
 */
Datum
aqo_show_classes(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hash_seq;
	ProfileMemEntry *entry;
    TupleDesc tupdesc;
    AttInMetadata *attinmeta;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	attinmeta = TupleDescGetAttInMetadata(tupdesc);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (aqo_profile_classes <= 0)
	{
		ReleaseTupleDesc(tupdesc);
		tuplestore_donestoring(tupstore);
		elog(WARNING, "Hash table 'profile_mem_queries' doesn't exist");
		PG_RETURN_VOID();
	}

	hash_seq_init(&hash_seq, profile_mem_queries);
	while (((entry = (ProfileMemEntry *) hash_seq_search(&hash_seq)) != NULL))
	{
		Datum values[3];
		bool  nulls[3] = {0, 0, 0};

		values[0] = UInt64GetDatum(entry->key);
		values[1] = Float8GetDatum(entry->time);
		values[2] = UInt32GetDatum(entry->counter);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	ReleaseTupleDesc(tupdesc);
	tuplestore_donestoring(tupstore);

	PG_RETURN_VOID();
}

/*
 * Clear the hash table. Return a number of deleted entries. Just for info.
 */
long
profile_clear_hash_table(void)
{
	HASH_SEQ_STATUS status;
	ProfileMemEntry *entry;
	long deleted;

	Assert(aqo_profile_classes > 0);

	/* callback only gets registered after creating the hash */
	Assert(profile_mem_queries != NULL);
	deleted = hash_get_num_entries(profile_mem_queries);

	if (deleted == 0)
		/* Fast path. Table is empty yet. */
		return 0;

	hash_seq_init(&status, profile_mem_queries);
	while ((entry = (ProfileMemEntry *) hash_seq_search(&status)) != NULL)
	{
		if (hash_search(profile_mem_queries,
						(void *) &entry->key,
						HASH_REMOVE,
						NULL) == NULL)
			elog(ERROR, "AQO: hash table corrupted");
	}

	Assert(hash_get_num_entries(profile_mem_queries) == 0);
	return deleted;
}

Datum
aqo_clear_classes(PG_FUNCTION_ARGS)
{
	int64 deleted = -1;

	if (aqo_profile_classes > 0)
		deleted = profile_clear_hash_table();

	PG_RETURN_INT64(deleted);
}

/*
 * Change entry or add new, if necessary.
 * Access to this shared hash table must be processed under a custom AQO lock,
 * related to a query hash value.
 */
void
update_profile_mem_table(double total_time)
{
	bool found;
	ProfileMemEntry *pentry;

	if (aqo_profile_classes <= 0 || !aqo_profile_enable)
		return;

	Assert(profile_mem_queries);

	pentry = (ProfileMemEntry *) hash_search(profile_mem_queries,
											 &query_context.query_hash,
											 HASH_ENTER_NULL, &found);

	if (pentry == NULL)
	{
		/* Out of memory. */
		elog(LOG, "AQO: profiling buffer is full.");
		return;
	}

	if (!found)
	{
		pentry->time = 0;
		pentry->counter = 0;
	}

	pentry->time += total_time;
	pentry->counter++;
}

/*
 * Estimate shared memory space needed.
 */
static Size
profile_memsize(void)
{
	Size		size = 0;

	Assert(aqo_profile_classes > 0);

	size = add_size(size,
					hash_estimate_size(aqo_profile_classes,
					sizeof(ProfileMemEntry)));
	return size;
}

void
profile_init(void)
{
	if (aqo_profile_classes <= 0)
		return;

	RequestAddinShmemSpace(profile_memsize());
}

/*
 * shmem_startup hook: allocate or attach to shared memory.
 * Allocate and initialize profiling-related shared memory, if not already
 * done, and set up backend-local pointer to that state.  Returns false if this
 * operation was failed.
 */
void
profile_shmem_startup(void)
{
	HASHCTL ctl;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	if (aqo_profile_classes <= 0)
		return;

	ctl.keysize = sizeof(uint64);
	ctl.entrysize = sizeof(ProfileMemEntry);
	profile_mem_queries = ShmemInitHash("aqo_profile_mem_queries",
										aqo_profile_classes,
										aqo_profile_classes,
										&ctl,
										HASH_ELEM | HASH_BLOBS);
}