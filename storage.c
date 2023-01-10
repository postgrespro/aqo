/*
 *******************************************************************************
 *
 *	STORAGE INTERACTION
 *
 * This module is responsible for interaction with the storage of AQO data.
 * It does not provide information protection from concurrent updates.
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/storage.c
 *
 */

#include "postgres.h"

#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "aqo.h"
#include "aqo_shared.h"
#include "machine_learning.h"
#include "preprocessing.h"
#include "storage.h"


/* AQO storage file names */
#define PGAQO_STAT_FILE	PGSTAT_STAT_PERMANENT_DIRECTORY "/pgaqo_statistics.stat"
#define PGAQO_TEXT_FILE	PGSTAT_STAT_PERMANENT_DIRECTORY "/pgaqo_query_texts.stat"
#define PGAQO_DATA_FILE	PGSTAT_STAT_PERMANENT_DIRECTORY "/pgaqo_data.stat"
#define PGAQO_QUERIES_FILE	PGSTAT_STAT_PERMANENT_DIRECTORY "/pgaqo_queries.stat"

#define AQO_DATA_COLUMNS			(7)
#define FormVectorSz(v_name)		(form_vector((v_name), (v_name ## _size)))


typedef enum {
	QUERYID = 0, EXEC_TIME_AQO, EXEC_TIME, PLAN_TIME_AQO, PLAN_TIME,
	EST_ERROR_AQO, EST_ERROR, NEXECS_AQO, NEXECS, TOTAL_NCOLS
} aqo_stat_cols;

typedef enum {
	QT_QUERYID = 0, QT_QUERY_STRING, QT_TOTAL_NCOLS
} aqo_qtexts_cols;

typedef enum {
	AD_FS = 0, AD_FSS, AD_NFEATURES, AD_FEATURES, AD_TARGETS, AD_RELIABILITY,
	AD_OIDS, AD_TOTAL_NCOLS
} aqo_data_cols;

typedef enum {
	AQ_QUERYID = 0, AQ_FS, AQ_LEARN_AQO, AQ_USE_AQO, AQ_AUTO_TUNING,
	AQ_TOTAL_NCOLS
} aqo_queries_cols;

typedef void* (*form_record_t) (void *ctx, size_t *size);
typedef bool (*deform_record_t) (void *data, size_t size);


int querytext_max_size = 1000;
int dsm_size_max = 100; /* in MB */

HTAB *stat_htab = NULL;
HTAB *queries_htab = NULL;
HTAB *qtexts_htab = NULL;
dsa_area *qtext_dsa = NULL;
HTAB *data_htab = NULL;
dsa_area *data_dsa = NULL;
HTAB *deactivated_queries = NULL;

/* Used to check data file consistency */
static const uint32 PGAQO_FILE_HEADER = 123467589;
static const uint32 PGAQO_PG_MAJOR_VERSION = PG_VERSION_NUM / 100;


static ArrayType *form_matrix(double *matrix, int nrows, int ncols);
static void dsa_init(void);
static int data_store(const char *filename, form_record_t callback,
					  long nrecs, void *ctx);
static void data_load(const char *filename, deform_record_t callback, void *ctx);
static size_t _compute_data_dsa(const DataEntry *entry);

static bool _aqo_stat_remove(uint64 queryid);
static bool _aqo_queries_remove(uint64 queryid);
static bool _aqo_qtexts_remove(uint64 queryid);
static bool _aqo_data_remove(data_key *key);

PG_FUNCTION_INFO_V1(aqo_query_stat);
PG_FUNCTION_INFO_V1(aqo_query_texts);
PG_FUNCTION_INFO_V1(aqo_data);
PG_FUNCTION_INFO_V1(aqo_queries);
PG_FUNCTION_INFO_V1(aqo_enable_query);
PG_FUNCTION_INFO_V1(aqo_disable_query);
PG_FUNCTION_INFO_V1(aqo_queries_update);
PG_FUNCTION_INFO_V1(aqo_reset);
PG_FUNCTION_INFO_V1(aqo_cleanup);
PG_FUNCTION_INFO_V1(aqo_drop_class);
PG_FUNCTION_INFO_V1(aqo_cardinality_error);
PG_FUNCTION_INFO_V1(aqo_execution_time);


bool
load_fss_ext(uint64 fs, int fss, OkNNrdata *data, List **reloids)
{
	return load_aqo_data(fs, fss, data, reloids, false);
}

bool
update_fss_ext(uint64 fs, int fss, OkNNrdata *data, List *reloids)
{
	return aqo_data_store(fs, fss, data, reloids);
}

/*
 * Forms ArrayType object for storage from simple C-array matrix.
 */
ArrayType *
form_matrix(double *matrix, int nrows, int ncols)
{
	Datum	   *elems;
	ArrayType  *array;
	int			dims[2] = {nrows, ncols};
	int			lbs[2];
	int			i,
				j;

	lbs[0] = lbs[1] = 1;
	elems = palloc(sizeof(*elems) * nrows * ncols);
	for (i = 0; i < nrows; ++i)
		for (j = 0; j < ncols; ++j)
		{
			elems[i * ncols + j] = Float8GetDatum(matrix[i * ncols + j]);
			Assert(!isnan(matrix[i * ncols + j]));
		}

	array = construct_md_array(elems, NULL, 2, dims, lbs,
							   FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd');
	return array;
}

/*
 * Forms ArrayType object for storage from simple C-array vector.
 */
static ArrayType *
form_vector(double *vector, int nrows)
{
	Datum	   *elems;
	ArrayType  *array;
	int			dims[1];
	int			lbs[1];
	int			i;

	dims[0] = nrows;
	lbs[0] = 1;
	elems = palloc(sizeof(*elems) * nrows);
	for (i = 0; i < nrows; ++i)
		elems[i] = Float8GetDatum(vector[i]);
	array = construct_md_array(elems, NULL, 1, dims, lbs,
							   FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd');
	return array;
}

/* Creates a storage for hashes of deactivated queries */
void
init_deactivated_queries_storage(void)
{
	HASHCTL		hash_ctl;

	/* Create the hashtable proper */
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(uint64);
	hash_ctl.entrysize = sizeof(uint64);
	deactivated_queries = hash_create("aqo_deactivated_queries",
									  128,		/* start small and extend */
									  &hash_ctl,
									  HASH_ELEM | HASH_BLOBS);
}

/* Checks whether the query with given hash is deactivated */
bool
query_is_deactivated(uint64 queryid)
{
	bool		found;

	hash_search(deactivated_queries, &queryid, HASH_FIND, &found);
	return found;
}

/* Adds given query hash into the set of hashes of deactivated queries */
void
add_deactivated_query(uint64 queryid)
{
	hash_search(deactivated_queries, &queryid, HASH_ENTER, NULL);
}

/*
 * Update AQO statistics.
 *
 * Add a record (or update an existed) to stat storage for the query class.
 * Returns a copy of stat entry, allocated in current memory context. Caller is
 * in charge to free this struct after usage.
 * If stat hash table is full, return NULL and log this fact.
 */
StatEntry *
aqo_stat_store(uint64 queryid, bool use_aqo,
			   double plan_time, double exec_time, double est_error)
{
	StatEntry  *entry;
	bool		found;
	int			pos;
	bool		tblOverflow;
	HASHACTION	action;

	Assert(stat_htab);

	LWLockAcquire(&aqo_state->stat_lock, LW_EXCLUSIVE);
	tblOverflow = hash_get_num_entries(stat_htab) < fs_max_items ? false : true;
	action = tblOverflow ? HASH_FIND : HASH_ENTER;
	entry = (StatEntry *) hash_search(stat_htab, &queryid, action, &found);

	/* Initialize entry on first usage */
	if (!found)
	{
		uint64 qid;

		if (action == HASH_FIND)
		{
			/*
			 * Hash table is full. To avoid possible problems - don't try to add
			 * more, just exit
			 */
			LWLockRelease(&aqo_state->stat_lock);
			ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("[AQO] Stat storage is full. No more feature spaces can be added."),
				 errhint("Increase value of aqo.fs_max_items on restart of the instance")));
			return NULL;
		}

		qid = entry->queryid;
		memset(entry, 0, sizeof(StatEntry));
		entry->queryid = qid;
	}

	/* Update the entry data */

	if (use_aqo)
	{
		Assert(entry->cur_stat_slot_aqo >= 0);
		pos = entry->cur_stat_slot_aqo;
		if (entry->cur_stat_slot_aqo < STAT_SAMPLE_SIZE - 1)
			entry->cur_stat_slot_aqo++;
		else
		{
			size_t sz = (STAT_SAMPLE_SIZE - 1) * sizeof(entry->est_error_aqo[0]);

			Assert(entry->cur_stat_slot_aqo = STAT_SAMPLE_SIZE - 1);
			memmove(entry->plan_time_aqo, &entry->plan_time_aqo[1], sz);
			memmove(entry->exec_time_aqo, &entry->exec_time_aqo[1], sz);
			memmove(entry->est_error_aqo, &entry->est_error_aqo[1], sz);
		}

		entry->execs_with_aqo++;
		entry->plan_time_aqo[pos] = plan_time;
		entry->exec_time_aqo[pos] = exec_time;
		entry->est_error_aqo[pos] = est_error;
	}
	else
	{
		Assert(entry->cur_stat_slot >= 0);
		pos = entry->cur_stat_slot;
		if (entry->cur_stat_slot < STAT_SAMPLE_SIZE - 1)
			entry->cur_stat_slot++;
		else
		{
			size_t sz = (STAT_SAMPLE_SIZE - 1) * sizeof(entry->est_error[0]);

			Assert(entry->cur_stat_slot = STAT_SAMPLE_SIZE - 1);
			memmove(entry->plan_time, &entry->plan_time[1], sz);
			memmove(entry->exec_time, &entry->exec_time[1], sz);
			memmove(entry->est_error, &entry->est_error[1], sz);
		}

		entry->execs_without_aqo++;
		entry->plan_time[pos] = plan_time;
		entry->exec_time[pos] = exec_time;
		entry->est_error[pos] = est_error;
	}

	entry = memcpy(palloc(sizeof(StatEntry)), entry, sizeof(StatEntry));
	aqo_state->stat_changed = true;
	LWLockRelease(&aqo_state->stat_lock);
	return entry;
}

/*
 * Returns AQO statistics on controlled query classes.
 */
Datum
aqo_query_stat(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupDesc;
	MemoryContext		per_query_ctx;
	MemoryContext		oldcontext;
	Tuplestorestate	   *tupstore;
	Datum				values[TOTAL_NCOLS + 1];
	bool				nulls[TOTAL_NCOLS + 1];
	HASH_SEQ_STATUS		hash_seq;
	StatEntry	   *entry;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	Assert(tupDesc->natts == TOTAL_NCOLS);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupDesc;

	MemoryContextSwitchTo(oldcontext);

	memset(nulls, 0, TOTAL_NCOLS + 1);
	LWLockAcquire(&aqo_state->stat_lock, LW_SHARED);
	hash_seq_init(&hash_seq, stat_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		memset(nulls, 0, TOTAL_NCOLS + 1);

		values[QUERYID] = Int64GetDatum(entry->queryid);
		values[NEXECS] = Int64GetDatum(entry->execs_without_aqo);
		values[NEXECS_AQO] = Int64GetDatum(entry->execs_with_aqo);
		values[EXEC_TIME_AQO] = PointerGetDatum(form_vector(entry->exec_time_aqo, entry->cur_stat_slot_aqo));
		values[EXEC_TIME] = PointerGetDatum(form_vector(entry->exec_time, entry->cur_stat_slot));
		values[PLAN_TIME_AQO] = PointerGetDatum(form_vector(entry->plan_time_aqo, entry->cur_stat_slot_aqo));
		values[PLAN_TIME] = PointerGetDatum(form_vector(entry->plan_time, entry->cur_stat_slot));
		values[EST_ERROR_AQO] = PointerGetDatum(form_vector(entry->est_error_aqo, entry->cur_stat_slot_aqo));
		values[EST_ERROR] = PointerGetDatum(form_vector(entry->est_error, entry->cur_stat_slot));
		tuplestore_putvalues(tupstore, tupDesc, values, nulls);
	}

	LWLockRelease(&aqo_state->stat_lock);
	tuplestore_donestoring(tupstore);
	return (Datum) 0;
}

static long
aqo_stat_reset(void)
{
	HASH_SEQ_STATUS	hash_seq;
	StatEntry	   *entry;
	long			num_remove = 0;
	long			num_entries;

	LWLockAcquire(&aqo_state->stat_lock, LW_EXCLUSIVE);
	num_entries = hash_get_num_entries(stat_htab);
	hash_seq_init(&hash_seq, stat_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (!hash_search(stat_htab, &entry->queryid, HASH_REMOVE, NULL))
			elog(PANIC, "[AQO] hash table corrupted");
		num_remove++;
	}
	aqo_state->stat_changed = true;
	LWLockRelease(&aqo_state->stat_lock);

	if (num_remove != num_entries)
		elog(ERROR, "[AQO] Stat memory storage is corrupted or parallel access without a lock was detected.");

	aqo_stat_flush();

	return num_remove;
}

static void *
_form_stat_record_cb(void *ctx, size_t *size)
{
	HASH_SEQ_STATUS *hash_seq = (HASH_SEQ_STATUS *) ctx;
	StatEntry		*entry;

	*size = sizeof(StatEntry);
	entry = hash_seq_search(hash_seq);
	if (entry == NULL)
		return NULL;

	return memcpy(palloc(*size), entry, *size);
}

/* Implement data flushing according to pgss_shmem_shutdown() */

void
aqo_stat_flush(void)
{
	HASH_SEQ_STATUS	hash_seq;
	int				ret;
	long			entries;

	/* Use exclusive lock to prevent concurrent flushing in different backends. */
	LWLockAcquire(&aqo_state->stat_lock, LW_EXCLUSIVE);

	if (!aqo_state->stat_changed)
		/* Hash table wasn't changed, meaningless to store it in permanent storage */
		goto end;

	entries = hash_get_num_entries(stat_htab);
	hash_seq_init(&hash_seq, stat_htab);
	ret = data_store(PGAQO_STAT_FILE, _form_stat_record_cb, entries,
					 (void *) &hash_seq);
	if (ret != 0)
		hash_seq_term(&hash_seq);
	else
		/* Hash table and disk storage are now consistent */
		aqo_state->stat_changed = false;

end:
	LWLockRelease(&aqo_state->stat_lock);
}

static void *
_form_qtext_record_cb(void *ctx, size_t *size)
{
	HASH_SEQ_STATUS *hash_seq = (HASH_SEQ_STATUS *) ctx;
	QueryTextEntry	*entry;
	void		    *data;
	char			*query_string;
	char			*ptr;

	entry = hash_seq_search(hash_seq);
	if (entry == NULL)
		return NULL;

	Assert(DsaPointerIsValid(entry->qtext_dp));
	query_string = dsa_get_address(qtext_dsa, entry->qtext_dp);
	Assert(query_string != NULL);
	*size = sizeof(entry->queryid) + strlen(query_string) + 1;
	ptr = data = palloc(*size);
	Assert(ptr != NULL);
	memcpy(ptr, &entry->queryid, sizeof(entry->queryid));
	ptr += sizeof(entry->queryid);
	memcpy(ptr, query_string, strlen(query_string) + 1);
	return data;
}

void
aqo_qtexts_flush(void)
{
	HASH_SEQ_STATUS	hash_seq;
	int				ret;
	long			entries;

	dsa_init();
	LWLockAcquire(&aqo_state->qtexts_lock, LW_EXCLUSIVE);

	if (!aqo_state->qtexts_changed)
		/* XXX: mull over forced mode. */
		goto end;

	entries = hash_get_num_entries(qtexts_htab);
	hash_seq_init(&hash_seq, qtexts_htab);
	ret = data_store(PGAQO_TEXT_FILE, _form_qtext_record_cb, entries,
					 (void *) &hash_seq);
	if (ret != 0)
		hash_seq_term(&hash_seq);
	else
		/* Hash table and disk storage are now consistent */
		aqo_state->qtexts_changed = false;

end:
	LWLockRelease(&aqo_state->qtexts_lock);
}

/*
 * Getting a hash table iterator, return a newly allocated memory chunk and its
 * size for subsequent writing into storage.
 */
static void *
_form_data_record_cb(void *ctx, size_t *size)
{
	HASH_SEQ_STATUS	   *hash_seq = (HASH_SEQ_STATUS *) ctx;
	DataEntry		   *entry;
	char			   *data;
	char			   *ptr,
					   *dsa_ptr;
	size_t				sz;

	entry = hash_seq_search(hash_seq);
	if (entry == NULL)
		return NULL;

	/* Size of data is DataEntry (without DSA pointer) plus size of DSA chunk */
	sz = offsetof(DataEntry, data_dp) + _compute_data_dsa(entry);
	ptr = data = palloc(sz);

	/* Put the data into the chunk */

	/* Plane copy of all bytes of hash table entry */
	memcpy(ptr, entry, offsetof(DataEntry, data_dp));
	ptr += offsetof(DataEntry, data_dp);

	Assert(DsaPointerIsValid(entry->data_dp));
	dsa_ptr = (char *) dsa_get_address(data_dsa, entry->data_dp);
	Assert((sz - (ptr - data)) == _compute_data_dsa(entry));
	memcpy(ptr, dsa_ptr, sz - (ptr - data));
	*size = sz;
	return data;
}

void
aqo_data_flush(void)
{
	HASH_SEQ_STATUS	hash_seq;
	int				ret;
	long			entries;

	dsa_init();
	LWLockAcquire(&aqo_state->data_lock, LW_EXCLUSIVE);

	if (!aqo_state->data_changed)
		/* XXX: mull over forced mode. */
		goto end;

	entries = hash_get_num_entries(data_htab);
	hash_seq_init(&hash_seq, data_htab);
	ret = data_store(PGAQO_DATA_FILE, _form_data_record_cb, entries,
					 (void *) &hash_seq);
	if (ret != 0)
		/*
		 * Something happened and storing procedure hasn't finished walking
		 * along all records of the hash table.
		 */
		hash_seq_term(&hash_seq);
	else
		/* Hash table and disk storage are now consistent */
		aqo_state->data_changed = false;
end:
	LWLockRelease(&aqo_state->data_lock);
}

static void *
_form_queries_record_cb(void *ctx, size_t *size)
{
	HASH_SEQ_STATUS *hash_seq = (HASH_SEQ_STATUS *) ctx;
	QueriesEntry		*entry;

	*size = sizeof(QueriesEntry);
	entry = hash_seq_search(hash_seq);
	if (entry == NULL)
		return NULL;

	return memcpy(palloc(*size), entry, *size);
}

void
aqo_queries_flush(void)
{
	HASH_SEQ_STATUS	hash_seq;
	int				ret;
	long			entries;

	LWLockAcquire(&aqo_state->queries_lock, LW_EXCLUSIVE);

	if (!aqo_state->queries_changed)
		goto end;

	entries = hash_get_num_entries(queries_htab);
	hash_seq_init(&hash_seq, queries_htab);
	ret = data_store(PGAQO_QUERIES_FILE, _form_queries_record_cb, entries,
					 (void *) &hash_seq);
	if (ret != 0)
		hash_seq_term(&hash_seq);
	else
		/* Hash table and disk storage are now consistent */
		aqo_state->queries_changed = false;

end:
	LWLockRelease(&aqo_state->queries_lock);
}

static int
data_store(const char *filename, form_record_t callback,
		   long nrecs, void *ctx)
{
	FILE   *file;
	size_t	size;
	uint32	counter = 0;
	void   *data;
	char   *tmpfile;

	tmpfile = psprintf("%s.tmp", filename);
	file = AllocateFile(tmpfile, PG_BINARY_W);
	if (file == NULL)
		goto error;

	if (fwrite(&PGAQO_FILE_HEADER, sizeof(uint32), 1, file) != 1 ||
		fwrite(&PGAQO_PG_MAJOR_VERSION, sizeof(uint32), 1, file) != 1 ||
		fwrite(&nrecs, sizeof(long), 1, file) != 1)
		goto error;

	while ((data = callback(ctx, &size)) != NULL)
	{
		/* TODO: Add CRC code ? */
		if (fwrite(&size, sizeof(size), 1, file) != 1 ||
			fwrite(data, size, 1, file) != 1)
			goto error;
		counter++;
	}

	Assert(counter == nrecs);
	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/* Parallel (re)writing into a file haven't happen. */
	(void) durable_rename(tmpfile, filename, PANIC);
	elog(LOG, "[AQO] %d records stored in file %s.", counter, filename);
	return 0;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write AQO file \"%s\": %m", tmpfile)));

	if (file)
		FreeFile(file);
	unlink(tmpfile);
	pfree(tmpfile);
	return -1;
}

static bool
_deform_stat_record_cb(void *data, size_t size)
{
	bool		found;
	StatEntry  *entry;
	uint64		queryid;

	Assert(LWLockHeldByMeInMode(&aqo_state->stat_lock, LW_EXCLUSIVE));
	Assert(size == sizeof(StatEntry));

	queryid = ((StatEntry *) data)->queryid;
	entry = (StatEntry *) hash_search(stat_htab, &queryid, HASH_ENTER, &found);
	Assert(!found && entry);
	memcpy(entry, data, sizeof(StatEntry));
	return true;
}

void
aqo_stat_load(void)
{
	Assert(!LWLockHeldByMe(&aqo_state->stat_lock));

	LWLockAcquire(&aqo_state->stat_lock, LW_EXCLUSIVE);

	/* Load on postmaster sturtup. So no any concurrent actions possible here. */
	Assert(hash_get_num_entries(stat_htab) == 0);

	data_load(PGAQO_STAT_FILE, _deform_stat_record_cb, NULL);

	LWLockRelease(&aqo_state->stat_lock);
}

static bool
_check_dsa_validity(dsa_pointer ptr)
{
	if (DsaPointerIsValid(ptr))
		return true;

	elog(LOG, "[AQO] DSA Pointer isn't valid. Is the memory limit exceeded?");
	return false;
}

static bool
_deform_qtexts_record_cb(void *data, size_t size)
{
	bool			found;
	QueryTextEntry *entry;
	uint64			queryid = *(uint64 *) data;
	char		   *query_string = (char *) data + sizeof(queryid);
	size_t			len = size - sizeof(queryid);
	char		   *strptr;

	Assert(LWLockHeldByMeInMode(&aqo_state->qtexts_lock, LW_EXCLUSIVE));
	Assert(strlen(query_string) + 1 == len);
	entry = (QueryTextEntry *) hash_search(qtexts_htab, &queryid,
										   HASH_ENTER, &found);
	Assert(!found);

	entry->qtext_dp = dsa_allocate(qtext_dsa, len);
	if (!_check_dsa_validity(entry->qtext_dp))
	{
		/*
		 * DSA stuck into problems. Rollback changes. Return false in belief
		 * that caller recognize it and don't try to call us more.
		 */
		(void) hash_search(qtexts_htab, &queryid, HASH_REMOVE, NULL);
		return false;
	}

	strptr = (char *) dsa_get_address(qtext_dsa, entry->qtext_dp);
	strlcpy(strptr, query_string, len);
	return true;
}

void
aqo_qtexts_load(void)
{
	uint64	queryid = 0;
	bool	found;

	Assert(!LWLockHeldByMe(&aqo_state->qtexts_lock));
	Assert(qtext_dsa != NULL);

	LWLockAcquire(&aqo_state->qtexts_lock, LW_EXCLUSIVE);

	if (hash_get_num_entries(qtexts_htab) != 0)
	{
		/* Someone have done it concurrently. */
		elog(LOG, "[AQO] Another backend have loaded query texts concurrently.");
		LWLockRelease(&aqo_state->qtexts_lock);
		return;
	}

	data_load(PGAQO_TEXT_FILE, _deform_qtexts_record_cb, NULL);

	/* Check existence of default feature space */
	(void) hash_search(qtexts_htab, &queryid, HASH_FIND, &found);

	aqo_state->qtexts_changed = false; /* mem data consistent with disk */
	LWLockRelease(&aqo_state->qtexts_lock);

	if (!found)
	{
		if (!aqo_qtext_store(0, "COMMON feature space (do not delete!)"))
			elog(PANIC, "[AQO] DSA Initialization was unsuccessful");
	}
}

/*
 * Getting a data chunk from a caller, add a record into the 'ML data'
 * shmem hash table. Allocate and fill DSA chunk for variadic part of the data.
 */
static bool
_deform_data_record_cb(void *data, size_t size)
{
	bool		found;
	DataEntry  *fentry = (DataEntry *) data; /*Depends on a platform? */
	DataEntry  *entry;
	size_t		sz;
	char	   *ptr = (char *) data,
			   *dsa_ptr;

	Assert(ptr != NULL);
	Assert(LWLockHeldByMeInMode(&aqo_state->data_lock, LW_EXCLUSIVE));

	entry = (DataEntry *) hash_search(data_htab, &fentry->key,
									  HASH_ENTER, &found);
	Assert(!found);

	/* Copy fixed-size part of entry byte-by-byte even with caves */
	memcpy(entry, fentry, offsetof(DataEntry, data_dp));
	ptr += offsetof(DataEntry, data_dp);

	sz = _compute_data_dsa(entry);
	Assert(sz + offsetof(DataEntry, data_dp) == size);
	entry->data_dp = dsa_allocate(data_dsa, sz);

	if (!_check_dsa_validity(entry->data_dp))
	{
		/*
		 * DSA stuck into problems. Rollback changes. Return false in belief
		 * that caller recognize it and don't try to call us more.
		 */
		(void) hash_search(data_htab, &fentry->key, HASH_REMOVE, NULL);
		return false;
	}

	dsa_ptr = (char *) dsa_get_address(data_dsa, entry->data_dp);
	Assert(dsa_ptr != NULL);
	memcpy(dsa_ptr, ptr, sz);
	return true;
}

void
aqo_data_load(void)
{
	Assert(!LWLockHeldByMe(&aqo_state->data_lock));
	Assert(data_dsa != NULL);

	LWLockAcquire(&aqo_state->data_lock, LW_EXCLUSIVE);

	if (hash_get_num_entries(data_htab) != 0)
	{
		/* Someone have done it concurrently. */
		elog(LOG, "[AQO] Another backend have loaded query data concurrently.");
		LWLockRelease(&aqo_state->data_lock);
		return;
	}

	data_load(PGAQO_DATA_FILE, _deform_data_record_cb, NULL);

	aqo_state->data_changed = false; /* mem data is consistent with disk */
	LWLockRelease(&aqo_state->data_lock);
}

static bool
_deform_queries_record_cb(void *data, size_t size)
{
	bool			found;
	QueriesEntry  	*entry;
	uint64			queryid;

	Assert(LWLockHeldByMeInMode(&aqo_state->queries_lock, LW_EXCLUSIVE));
	Assert(size == sizeof(QueriesEntry));

	queryid = ((QueriesEntry *) data)->queryid;
	entry = (QueriesEntry *) hash_search(queries_htab, &queryid, HASH_ENTER, &found);
	Assert(!found);
	memcpy(entry, data, sizeof(QueriesEntry));
	return true;
}

void
aqo_queries_load(void)
{
	bool	found;
	uint64	queryid = 0;

	Assert(!LWLockHeldByMe(&aqo_state->queries_lock));

	LWLockAcquire(&aqo_state->queries_lock, LW_EXCLUSIVE);

	/* Load on postmaster startup. So no any concurrent actions possible here. */
	Assert(hash_get_num_entries(queries_htab) == 0);

	data_load(PGAQO_QUERIES_FILE, _deform_queries_record_cb, NULL);

	/* Check existence of default feature space */
	(void) hash_search(queries_htab, &queryid, HASH_FIND, &found);

	LWLockRelease(&aqo_state->queries_lock);
	if (!found)
	{
		if (!aqo_queries_store(0, 0, 0, 0, 0))
			elog(PANIC, "[AQO] aqo_queries initialization was unsuccessful");
	}
}

static void
data_load(const char *filename, deform_record_t callback, void *ctx)
{
	FILE   *file;
	long	i;
	uint32	header;
	int32	pgver;
	long	num;

	file = AllocateFile(filename, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno != ENOENT)
			goto read_error;
		return;
	}

	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		fread(&pgver, sizeof(uint32), 1, file) != 1 ||
		fread(&num, sizeof(long), 1, file) != 1)
		goto read_error;

	if (header != PGAQO_FILE_HEADER || pgver != PGAQO_PG_MAJOR_VERSION)
		goto data_error;

	for (i = 0; i < num; i++)
	{
		void   *data;
		size_t	size;
		bool	res;

		if (fread(&size, sizeof(size), 1, file) != 1)
			goto read_error;
		data = palloc(size);
		if (fread(data, size, 1, file) != 1)
			goto read_error;
		res = callback(data, size);

		if (!res)
		{
			/* Error detected. Do not try to read tails of the storage. */
			elog(LOG, "[AQO] Because of an error skip %ld storage records.",
				 num - i);
			break;
		}
	}

	FreeFile(file);

	elog(LOG, "[AQO] %ld records loaded from file %s.", num, filename);
	return;

read_error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read file \"%s\": %m", filename)));
	goto fail;
data_error:
	ereport(LOG,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("ignoring invalid data in file \"%s\"", filename)));
fail:
	if (file)
		FreeFile(file);
	unlink(filename);
}

static void
on_shmem_shutdown(int code, Datum arg)
{
	/*
	 * XXX: It can be expensive to rewrite a file on each shutdown of a backend.
	 */
	aqo_qtexts_flush();
	aqo_data_flush();
}

/*
 * Initialize DSA memory for AQO shared data with variable length.
 * On first call, create DSA segments and load data into hash table and DSA
 * from disk.
 */
static void
dsa_init()
{
	MemoryContext	old_context;

	if (qtext_dsa)
		return;

	Assert(data_dsa == NULL && data_dsa == NULL);
	old_context = MemoryContextSwitchTo(TopMemoryContext);
	LWLockAcquire(&aqo_state->lock, LW_EXCLUSIVE);

	if (aqo_state->qtexts_dsa_handler == DSM_HANDLE_INVALID)
	{
		Assert(aqo_state->data_dsa_handler == DSM_HANDLE_INVALID);

		qtext_dsa = dsa_create(aqo_state->qtext_trancheid);
		Assert(qtext_dsa != NULL);

		if (dsm_size_max > 0)
			dsa_set_size_limit(qtext_dsa, dsm_size_max * 1024 * 1024);

		dsa_pin(qtext_dsa);
		aqo_state->qtexts_dsa_handler = dsa_get_handle(qtext_dsa);

		data_dsa = qtext_dsa;
		aqo_state->data_dsa_handler = dsa_get_handle(data_dsa);

		/* Load and initialize query texts hash table */
		aqo_qtexts_load();
		aqo_data_load();
	}
	else
	{
		qtext_dsa = dsa_attach(aqo_state->qtexts_dsa_handler);
		data_dsa = qtext_dsa;
	}

	dsa_pin_mapping(qtext_dsa);
	MemoryContextSwitchTo(old_context);
	LWLockRelease(&aqo_state->lock);

	before_shmem_exit(on_shmem_shutdown, (Datum) 0);
}

/* ************************************************************************** */

/*
 * XXX: Maybe merge with aqo_queries ?
 */
bool
aqo_qtext_store(uint64 queryid, const char *query_string)
{
	QueryTextEntry *entry;
	bool			found;
	bool			tblOverflow;
	HASHACTION		action;

	Assert(!LWLockHeldByMe(&aqo_state->qtexts_lock));

	if (query_string == NULL || querytext_max_size == 0)
		return false;

	dsa_init();

	LWLockAcquire(&aqo_state->qtexts_lock, LW_EXCLUSIVE);

	/* Check hash table overflow */
	tblOverflow = hash_get_num_entries(qtexts_htab) < fs_max_items ? false : true;
	action = tblOverflow ? HASH_FIND : HASH_ENTER;

	entry = (QueryTextEntry *) hash_search(qtexts_htab, &queryid, action,
										   &found);

	/* Initialize entry on first usage */
	if (!found)
	{
		size_t size = strlen(query_string) + 1;
		char *strptr;

		if (action == HASH_FIND)
		{
			/*
			 * Hash table is full. To avoid possible problems - don't try to add
			 * more, just exit
			 */
			LWLockRelease(&aqo_state->qtexts_lock);
			ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("[AQO] Query texts storage is full. No more feature spaces can be added."),
				 errhint("Increase value of aqo.fs_max_items on restart of the instance")));
			return false;
		}

		entry->queryid = queryid;
		size = size > querytext_max_size ? querytext_max_size : size;
		entry->qtext_dp = dsa_allocate(qtext_dsa, size);

		if (!_check_dsa_validity(entry->qtext_dp))
		{
			/*
			 * DSA stuck into problems. Rollback changes. Return false in belief
			 * that caller recognize it and don't try to call us more.
			 */
			(void) hash_search(qtexts_htab, &queryid, HASH_REMOVE, NULL);
			LWLockRelease(&aqo_state->qtexts_lock);
			return false;
		}

		strptr = (char *) dsa_get_address(qtext_dsa, entry->qtext_dp);
		strlcpy(strptr, query_string, size);
		aqo_state->qtexts_changed = true;
	}
	LWLockRelease(&aqo_state->qtexts_lock);
	return true;
}

Datum
aqo_query_texts(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupDesc;
	MemoryContext		per_query_ctx;
	MemoryContext		oldcontext;
	Tuplestorestate	   *tupstore;
	Datum				values[QT_TOTAL_NCOLS];
	bool				nulls[QT_TOTAL_NCOLS];
	HASH_SEQ_STATUS		hash_seq;
	QueryTextEntry	   *entry;

	Assert(!LWLockHeldByMe(&aqo_state->qtexts_lock));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	Assert(tupDesc->natts == QT_TOTAL_NCOLS);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupDesc;

	MemoryContextSwitchTo(oldcontext);

	dsa_init();
	memset(nulls, 0, QT_TOTAL_NCOLS);
	LWLockAcquire(&aqo_state->qtexts_lock, LW_SHARED);
	hash_seq_init(&hash_seq, qtexts_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		char *ptr;

		Assert(DsaPointerIsValid(entry->qtext_dp));
		ptr = dsa_get_address(qtext_dsa, entry->qtext_dp);
		values[QT_QUERYID] = Int64GetDatum(entry->queryid);
		values[QT_QUERY_STRING] = CStringGetTextDatum(ptr);
		tuplestore_putvalues(tupstore, tupDesc, values, nulls);
	}

	LWLockRelease(&aqo_state->qtexts_lock);
	tuplestore_donestoring(tupstore);
	return (Datum) 0;
}

static bool
_aqo_stat_remove(uint64 queryid)
{
	bool		found;

	Assert(!LWLockHeldByMe(&aqo_state->stat_lock));
	LWLockAcquire(&aqo_state->stat_lock, LW_EXCLUSIVE);
	(void) hash_search(stat_htab, &queryid, HASH_FIND, &found);

	if (found)
	{
		(void) hash_search(stat_htab, &queryid, HASH_REMOVE, NULL);
		aqo_state->stat_changed = true;
	}

	LWLockRelease(&aqo_state->stat_lock);
	return found;
}

static bool
_aqo_queries_remove(uint64 queryid)
{
	bool	found;

	Assert(!LWLockHeldByMe(&aqo_state->queries_lock));
	LWLockAcquire(&aqo_state->queries_lock, LW_EXCLUSIVE);
	(void) hash_search(queries_htab, &queryid, HASH_FIND, &found);

	if (found)
	{
		(void) hash_search(queries_htab, &queryid, HASH_REMOVE, NULL);
		aqo_state->queries_changed = true;
	}

	LWLockRelease(&aqo_state->queries_lock);
	return found;
}

static bool
_aqo_qtexts_remove(uint64 queryid)
{
	bool			found = false;
	QueryTextEntry *entry;

	dsa_init();

	Assert(!LWLockHeldByMe(&aqo_state->qtexts_lock));
	LWLockAcquire(&aqo_state->qtexts_lock, LW_EXCLUSIVE);

	/*
	 * Look for a record with this queryid. DSA fields must be freed before
	 * deletion of the record.
	 */
	entry = (QueryTextEntry *) hash_search(qtexts_htab, &queryid, HASH_FIND,
										   &found);
	if (found)
	{
		/* Free DSA memory, allocated for this record */
		Assert(DsaPointerIsValid(entry->qtext_dp));
		dsa_free(qtext_dsa, entry->qtext_dp);

		(void) hash_search(qtexts_htab, &queryid, HASH_REMOVE, NULL);
		aqo_state->qtexts_changed = true;
	}

	LWLockRelease(&aqo_state->qtexts_lock);
	return found;
}

static bool
_aqo_data_remove(data_key *key)
{
	DataEntry  *entry;
	bool		found;

	Assert(!LWLockHeldByMe(&aqo_state->data_lock));
	LWLockAcquire(&aqo_state->data_lock, LW_EXCLUSIVE);

	entry = (DataEntry *) hash_search(data_htab, key, HASH_FIND, &found);
	if (found)
	{
		/* Free DSA memory, allocated for this record */
		Assert(DsaPointerIsValid(entry->data_dp));
		dsa_free(data_dsa, entry->data_dp);
		entry->data_dp = InvalidDsaPointer;

		if (!hash_search(data_htab, key, HASH_REMOVE, NULL))
			elog(PANIC, "[AQO] Inconsistent data hash table");

		aqo_state->data_changed = true;
	}

	LWLockRelease(&aqo_state->data_lock);
	return found;
}

static long
aqo_qtexts_reset(void)
{
	HASH_SEQ_STATUS	hash_seq;
	QueryTextEntry *entry;
	long			num_remove = 0;
	long			num_entries;

	dsa_init();

	Assert(!LWLockHeldByMe(&aqo_state->qtexts_lock));
	LWLockAcquire(&aqo_state->qtexts_lock, LW_EXCLUSIVE);
	num_entries = hash_get_num_entries(qtexts_htab);
	hash_seq_init(&hash_seq, qtexts_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (entry->queryid == 0)
			continue;

		Assert(DsaPointerIsValid(entry->qtext_dp));
		dsa_free(qtext_dsa, entry->qtext_dp);
		if (!hash_search(qtexts_htab, &entry->queryid, HASH_REMOVE, NULL))
			elog(PANIC, "[AQO] hash table corrupted");
		num_remove++;
	}
	aqo_state->qtexts_changed = true;
	LWLockRelease(&aqo_state->qtexts_lock);
	if (num_remove != num_entries - 1)
		elog(ERROR, "[AQO] Query texts memory storage is corrupted or parallel access without a lock was detected.");

	aqo_qtexts_flush();

	return num_remove;
}

static size_t
_compute_data_dsa(const DataEntry *entry)
{
	size_t	size = sizeof(data_key); /* header's size */

	size += sizeof(double) * entry->rows * entry->cols; /* matrix */
	size += 2 * sizeof(double) * entry->rows; /* targets, rfactors */

	/* Calculate memory size needed to store relation names */
	size += entry->nrels * sizeof(Oid);
	return size;
}

/*
 * Insert new record or update existed in the AQO data storage.
 * Return true if data was changed.
 */
bool
aqo_data_store(uint64 fs, int fss, OkNNrdata *data, List *reloids)
{
	DataEntry  *entry;
	bool		found;
	data_key	key = {.fs = fs, .fss = fss};
	int			i;
	char	   *ptr;
	ListCell   *lc;
	size_t		size;
	bool		tblOverflow;
	HASHACTION	action;
	bool		result;

	Assert(!LWLockHeldByMe(&aqo_state->data_lock));

	dsa_init();

	LWLockAcquire(&aqo_state->data_lock, LW_EXCLUSIVE);

	/* Check hash table overflow */
	tblOverflow = hash_get_num_entries(data_htab) < fss_max_items ? false : true;
	action = tblOverflow ? HASH_FIND : HASH_ENTER;

	entry = (DataEntry *) hash_search(data_htab, &key, action, &found);

	/* Initialize entry on first usage */
	if (!found)
	{
		if (action == HASH_FIND)
		{
			/*
			 * Hash table is full. To avoid possible problems - don't try to add
			 * more, just exit
			 */
			LWLockRelease(&aqo_state->data_lock);
			ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("[AQO] Data storage is full. No more data can be added."),
				 errhint("Increase value of aqo.fss_max_items on restart of the instance")));
			return false;
		}

		entry->cols = data->cols;
		entry->rows = data->rows;
		entry->nrels = list_length(reloids);

		size = _compute_data_dsa(entry);
		entry->data_dp = dsa_allocate0(data_dsa, size);

		if (!_check_dsa_validity(entry->data_dp))
		{
			/*
			 * DSA stuck into problems. Rollback changes. Return false in belief
			 * that caller recognize it and don't try to call us more.
			 */
			(void) hash_search(data_htab, &key, HASH_REMOVE, NULL);
			LWLockRelease(&aqo_state->data_lock);
			return false;
		}
	}

	Assert(DsaPointerIsValid(entry->data_dp));

	if (entry->cols != data->cols || entry->nrels != list_length(reloids))
	{
		/* Collision happened? */
		elog(LOG, "[AQO] Does a collision happened? Check it if possible (fs: "
			 UINT64_FORMAT", fss: %d).",
			 fs, fss);
		goto end;
	}

	if (entry->rows < data->rows)
	{
		entry->rows = data->rows;
		size = _compute_data_dsa(entry);

		/* Need to re-allocate DSA chunk */
		dsa_free(data_dsa, entry->data_dp);
		entry->data_dp = dsa_allocate0(data_dsa, size);

		if (!_check_dsa_validity(entry->data_dp))
		{
			/*
			 * DSA stuck into problems. Rollback changes. Return false in belief
			 * that caller recognize it and don't try to call us more.
			 */
			(void) hash_search(data_htab, &key, HASH_REMOVE, NULL);
			LWLockRelease(&aqo_state->data_lock);
			return false;
		}
	}
	ptr = (char *) dsa_get_address(data_dsa, entry->data_dp);
	Assert(ptr != NULL);

	/*
	 * Copy AQO data into allocated DSA segment
	 */

	memcpy(ptr, &key, sizeof(data_key)); /* Just for debug */
	ptr += sizeof(data_key);
	if (entry->cols > 0)
	{
		for (i = 0; i < entry->rows; i++)
		{
			Assert(data->matrix[i]);
			memcpy(ptr, data->matrix[i], sizeof(double) * data->cols);
			ptr += sizeof(double) * data->cols;
		}
	}
	/* copy targets into DSM storage */
	memcpy(ptr, data->targets, sizeof(double) * entry->rows);
	ptr += sizeof(double) * entry->rows;
	/* copy rfactors into DSM storage */
	memcpy(ptr, data->rfactors, sizeof(double) * entry->rows);
	ptr += sizeof(double) * entry->rows;
	/* store list of relations. XXX: optimize ? */
	foreach(lc, reloids)
	{
		Oid reloid = lfirst_oid(lc);

		memcpy(ptr, &reloid, sizeof(Oid));
		ptr += sizeof(Oid);
	}

	aqo_state->data_changed = true;
end:
	result = aqo_state->data_changed;
	LWLockRelease(&aqo_state->data_lock);
	return result;
}

static void
build_knn_matrix(OkNNrdata *data, const OkNNrdata *temp_data)
{
	Assert(data->cols == temp_data->cols);
	Assert(data->matrix);

	if (data->rows > 0)
		/* trivial strategy - use first suitable record and ignore others */
		return;

	memcpy(data, temp_data, sizeof(OkNNrdata));
	if (data->cols > 0)
	{
		int i;

		for (i = 0; i < data->rows; i++)
		{
			Assert(data->matrix[i]);
			memcpy(data->matrix[i], temp_data->matrix[i], data->cols * sizeof(double));
		}
	}
}

static OkNNrdata *
_fill_knn_data(const DataEntry *entry, List **reloids)
{
	OkNNrdata *data;
	char	   *ptr;
	int			i;
	size_t		offset;
	size_t		sz = _compute_data_dsa(entry);

	data = OkNNr_allocate(entry->cols);
	data->rows = entry->rows;

	ptr = (char *) dsa_get_address(data_dsa, entry->data_dp);

	/* Check invariants */
	Assert(entry->rows <= aqo_K);
	Assert(ptr != NULL);
	Assert(entry->key.fss == ((data_key *)ptr)->fss);
	Assert(data->matrix);

	ptr += sizeof(data_key);

	if (entry->cols > 0)
	{
		for (i = 0; i < entry->rows; i++)
		{
			Assert(data->matrix[i]);
			memcpy(data->matrix[i], ptr, sizeof(double) * data->cols);
			ptr += sizeof(double) * data->cols;
		}
	}

	/* copy targets from DSM storage */
	memcpy(data->targets, ptr, sizeof(double) * entry->rows);
	ptr += sizeof(double) * entry->rows;
	offset = ptr - (char *) dsa_get_address(data_dsa, entry->data_dp);
	Assert(offset < sz);

	/* copy rfactors from DSM storage */
	memcpy(data->rfactors, ptr, sizeof(double) * entry->rows);
	ptr += sizeof(double) * entry->rows;
	offset = ptr - (char *) dsa_get_address(data_dsa, entry->data_dp);
	Assert(offset <= sz);

	if (reloids == NULL)
		/* Isn't needed to load reloids list */
		return data;

	/* store list of relations. XXX: optimize ? */
	for (i = 0; i < entry->nrels; i++)
	{
		*reloids = lappend_oid(*reloids, ObjectIdGetDatum(*(Oid*)ptr));
		ptr += sizeof(Oid);
	}

	offset = ptr - (char *) dsa_get_address(data_dsa, entry->data_dp);
	if (offset != sz)
		elog(PANIC, "[AQO] Shared memory ML storage is corrupted.");

	return data;
}

/*
 * Return on feature subspace, unique defined by its class (fs) and hash value
 * (fss).
 * If reloids is NULL, skip loading of this list.
 * If wideSearch is true - make seqscan on the hash table to see for relevant
 * data across neighbours.
 */
bool
load_aqo_data(uint64 fs, int fss, OkNNrdata *data, List **reloids,
			  bool wideSearch)
{
	DataEntry  *entry;
	bool		found;
	data_key	key = {.fs = fs, .fss = fss};
	OkNNrdata  *temp_data;

	Assert(!LWLockHeldByMe(&aqo_state->data_lock));

	dsa_init();

	LWLockAcquire(&aqo_state->data_lock, LW_SHARED);

	if (!wideSearch)
	{
		entry = (DataEntry *) hash_search(data_htab, &key, HASH_FIND, &found);

		if (!found)
			goto end;

		/* One entry with all correctly filled fields is found */
		Assert(entry);
		Assert(DsaPointerIsValid(entry->data_dp));

		if (entry->cols != data->cols)
		{
			/* Collision happened? */
			elog(LOG, "[AQO] Does a collision happened? Check it if possible "
				 "(fs: "UINT64_FORMAT", fss: %d).",
				 fs, fss);
			found = false;
			goto end;
		}

		temp_data = _fill_knn_data(entry, reloids);
		build_knn_matrix(data, temp_data);
	}
	else
	/* Iterate across all elements of the table. XXX: Maybe slow. */
	{
		HASH_SEQ_STATUS	hash_seq;
		int				noids = -1;

		found = false;
		hash_seq_init(&hash_seq, data_htab);
		while ((entry = hash_seq_search(&hash_seq)) != NULL)
		{
			List *tmp_oids = NIL;

			if (entry->key.fss != fss || entry->cols != data->cols)
				continue;

			temp_data = _fill_knn_data(entry, &tmp_oids);

			if (data->rows > 0 && list_length(tmp_oids) != noids)
			{
				/* Dubious case. So log it and skip these data */
				elog(LOG,
					 "[AQO] different number depended oids for the same fss %d: "
					 "%d and %d correspondingly.",
					 fss, list_length(tmp_oids), noids);
				Assert(noids >= 0);
				list_free(tmp_oids);
				continue;
			}

			noids = list_length(tmp_oids);

			if (reloids != NULL && *reloids == NIL)
				*reloids = tmp_oids;
			else
				list_free(tmp_oids);

			build_knn_matrix(data, temp_data);
			found = true;
		}
	}

	Assert(!found || (data->rows > 0 && data->rows <= aqo_K));
end:
	LWLockRelease(&aqo_state->data_lock);

	return found;
}

Datum
aqo_data(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupDesc;
	MemoryContext		per_query_ctx;
	MemoryContext		oldcontext;
	Tuplestorestate	   *tupstore;
	Datum				values[AD_TOTAL_NCOLS];
	bool				nulls[AD_TOTAL_NCOLS];
	HASH_SEQ_STATUS		hash_seq;
	DataEntry		   *entry;

	Assert(!LWLockHeldByMe(&aqo_state->data_lock));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	Assert(tupDesc->natts == AD_TOTAL_NCOLS);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupDesc;

	MemoryContextSwitchTo(oldcontext);

	dsa_init();
	LWLockAcquire(&aqo_state->data_lock, LW_SHARED);
	hash_seq_init(&hash_seq, data_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		char *ptr;

		memset(nulls, 0, AD_TOTAL_NCOLS);

		values[AD_FS] = Int64GetDatum(entry->key.fs);
		values[AD_FSS] = Int32GetDatum((int) entry->key.fss);
		values[AD_NFEATURES] = Int32GetDatum(entry->cols);

		/* Fill values from the DSA data chunk */
		Assert(DsaPointerIsValid(entry->data_dp));
		ptr = dsa_get_address(data_dsa, entry->data_dp);
		Assert(entry->key.fs == ((data_key*)ptr)->fs && entry->key.fss == ((data_key*)ptr)->fss);
		ptr += sizeof(data_key);

		if (entry->cols > 0)
			values[AD_FEATURES] = PointerGetDatum(form_matrix((double *) ptr,
													entry->rows, entry->cols));
		else
			nulls[AD_FEATURES] = true;

		ptr += sizeof(double) * entry->rows * entry->cols;
		values[AD_TARGETS] = PointerGetDatum(form_vector((double *)ptr, entry->rows));
		ptr += sizeof(double) * entry->rows;
		values[AD_RELIABILITY] = PointerGetDatum(form_vector((double *)ptr, entry->rows));
		ptr += sizeof(double) * entry->rows;

		if (entry->nrels > 0)
		{
			Datum	   *elems;
			ArrayType  *array;
			int			i;

			elems = palloc(sizeof(*elems) * entry->nrels);
			for(i = 0; i < entry->nrels; i++)
			{
				elems[i] = ObjectIdGetDatum(*(Oid *)ptr);
				ptr += sizeof(Oid);
			}

			array = construct_array(elems, entry->nrels, OIDOID,
									sizeof(Oid), true, TYPALIGN_INT);
			values[AD_OIDS] = PointerGetDatum(array);
		}
		else
			nulls[AD_OIDS] = true;

		tuplestore_putvalues(tupstore, tupDesc, values, nulls);
	}

	LWLockRelease(&aqo_state->data_lock);
	tuplestore_donestoring(tupstore);
	return (Datum) 0;
}

static long
_aqo_data_clean(uint64 fs)
{
	HASH_SEQ_STATUS	hash_seq;
	DataEntry	   *entry;
	long			removed = 0;

	Assert(!LWLockHeldByMe(&aqo_state->data_lock));
	LWLockAcquire(&aqo_state->data_lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, data_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (entry->key.fs != fs)
			continue;

		Assert(DsaPointerIsValid(entry->data_dp));
		dsa_free(data_dsa, entry->data_dp);
		entry->data_dp = InvalidDsaPointer;
		if (!hash_search(data_htab, &entry->key, HASH_REMOVE, NULL))
			elog(PANIC, "[AQO] hash table corrupted");
		removed++;
	}

	LWLockRelease(&aqo_state->data_lock);
	return removed;
}

static long
aqo_data_reset(void)
{
	HASH_SEQ_STATUS	hash_seq;
	DataEntry	   *entry;
	long			num_remove = 0;
	long			num_entries;

	dsa_init();

	Assert(!LWLockHeldByMe(&aqo_state->data_lock));
	LWLockAcquire(&aqo_state->data_lock, LW_EXCLUSIVE);
	num_entries = hash_get_num_entries(data_htab);
	hash_seq_init(&hash_seq, data_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Assert(DsaPointerIsValid(entry->data_dp));
		dsa_free(data_dsa, entry->data_dp);
		if (!hash_search(data_htab, &entry->key, HASH_REMOVE, NULL))
			elog(PANIC, "[AQO] hash table corrupted");
		num_remove++;
	}

	if (num_remove > 0)
		aqo_state->data_changed = true;
	LWLockRelease(&aqo_state->data_lock);
	if (num_remove != num_entries)
		elog(ERROR, "[AQO] Query ML memory storage is corrupted or parallel access without a lock has detected.");

	aqo_data_flush();

	return num_remove;
}

Datum
aqo_queries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupDesc;
	MemoryContext		per_query_ctx;
	MemoryContext		oldcontext;
	Tuplestorestate	   *tupstore;
	Datum				values[AQ_TOTAL_NCOLS + 1];
	bool				nulls[AQ_TOTAL_NCOLS + 1];
	HASH_SEQ_STATUS		hash_seq;
	QueriesEntry	   *entry;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	Assert(tupDesc->natts == AQ_TOTAL_NCOLS);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupDesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(&aqo_state->queries_lock, LW_SHARED);
	hash_seq_init(&hash_seq, queries_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		memset(nulls, 0, AQ_TOTAL_NCOLS + 1);

		values[AQ_QUERYID] = Int64GetDatum(entry->queryid);
		values[AQ_FS] = Int64GetDatum(entry->fs);
		values[AQ_LEARN_AQO] = BoolGetDatum(entry->learn_aqo);
		values[AQ_USE_AQO] = BoolGetDatum(entry->use_aqo);
		values[AQ_AUTO_TUNING] = BoolGetDatum(entry->auto_tuning);
		tuplestore_putvalues(tupstore, tupDesc, values, nulls);
	}

	LWLockRelease(&aqo_state->queries_lock);
	tuplestore_donestoring(tupstore);
	return (Datum) 0;
}

bool
aqo_queries_store(uint64 queryid,
				  uint64 fs, bool learn_aqo, bool use_aqo, bool auto_tuning)
{
	QueriesEntry   *entry;
	bool			found;
	bool		tblOverflow;
	HASHACTION	action;

	Assert(queries_htab);

	/* Guard for default feature space */
	Assert(queryid != 0 || (fs == 0 && learn_aqo == false &&
		   use_aqo == false && auto_tuning == false));

	LWLockAcquire(&aqo_state->queries_lock, LW_EXCLUSIVE);

	/* Check hash table overflow */
	tblOverflow = hash_get_num_entries(queries_htab) < fs_max_items ? false : true;
	action = tblOverflow ? HASH_FIND : HASH_ENTER;

	entry = (QueriesEntry *) hash_search(queries_htab, &queryid, action,
										 &found);

		/* Initialize entry on first usage */
	if (!found && action == HASH_FIND)
	{
		/*
		 * Hash table is full. To avoid possible problems - don't try to add
		 * more, just exit
		 */
		LWLockRelease(&aqo_state->queries_lock);
		ereport(LOG,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("[AQO] Queries storage is full. No more feature spaces can be added."),
			 errhint("Increase value of aqo.fs_max_items on restart of the instance")));
		return false;
	}

	entry->fs = fs;
	entry->learn_aqo = learn_aqo;
	entry->use_aqo = use_aqo;
	entry->auto_tuning = auto_tuning;

	aqo_state->queries_changed = true;
	LWLockRelease(&aqo_state->queries_lock);
	return true;
}

static long
aqo_queries_reset(void)
{
	HASH_SEQ_STATUS		hash_seq;
	QueriesEntry	   *entry;
	long				num_remove = 0;
	long				num_entries;

	LWLockAcquire(&aqo_state->queries_lock, LW_EXCLUSIVE);
	num_entries = hash_get_num_entries(queries_htab);
	hash_seq_init(&hash_seq, queries_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (entry->queryid == 0)
			/* Don't remove default feature space */
			continue;

		if (!hash_search(queries_htab, &entry->queryid, HASH_REMOVE, NULL))
			elog(PANIC, "[AQO] hash table corrupted");
		num_remove++;
	}

	if (num_remove > 0)
		aqo_state->queries_changed = true;

	LWLockRelease(&aqo_state->queries_lock);

	if (num_remove != num_entries - 1)
		elog(ERROR, "[AQO] Queries memory storage is corrupted or parallel access without a lock has detected.");

	aqo_queries_flush();

	return num_remove;
}

Datum
aqo_enable_query(PG_FUNCTION_ARGS)
{
	uint64			queryid = (uint64) PG_GETARG_INT64(0);
	QueriesEntry   *entry;
	bool			found;

	Assert(queries_htab);

	if (queryid == 0)
		elog(ERROR, "[AQO] Default class can't be updated.");

	LWLockAcquire(&aqo_state->queries_lock, LW_EXCLUSIVE);
	entry = (QueriesEntry *) hash_search(queries_htab, &queryid, HASH_FIND, &found);

	if (found)
	{
		entry->learn_aqo = true;
		entry->use_aqo = true;
		if (aqo_mode == AQO_MODE_INTELLIGENT)
			entry->auto_tuning = true;
	}
	else
		elog(ERROR, "[AQO] Entry with queryid "INT64_FORMAT
			 " not contained in table", (int64) queryid);

	hash_search(deactivated_queries, &queryid, HASH_REMOVE, NULL);
	LWLockRelease(&aqo_state->queries_lock);
	PG_RETURN_VOID();
}

Datum
aqo_disable_query(PG_FUNCTION_ARGS)
{
	uint64			queryid = (uint64) PG_GETARG_INT64(0);
	QueriesEntry   *entry;
	bool			found;

	Assert(queries_htab);

	LWLockAcquire(&aqo_state->queries_lock, LW_EXCLUSIVE);
	entry = (QueriesEntry *) hash_search(queries_htab, &queryid, HASH_FIND, &found);

	if(found)
	{
		entry->learn_aqo = false;
		entry->use_aqo = false;
		entry->auto_tuning = false;
	}
	else
	{
		elog(ERROR, "[AQO] Entry with "INT64_FORMAT" not contained in table",
			 (int64) queryid);
	}
	LWLockRelease(&aqo_state->queries_lock);
	PG_RETURN_VOID();
}

bool
aqo_queries_find(uint64 queryid, QueryContextData *ctx)
{
	bool			found;
	QueriesEntry   *entry;

	Assert(queries_htab);

	LWLockAcquire(&aqo_state->queries_lock, LW_SHARED);
	entry = (QueriesEntry *) hash_search(queries_htab, &queryid, HASH_FIND, &found);
	if (found)
	{
		ctx->query_hash = entry->queryid;
		ctx->learn_aqo = entry->learn_aqo;
		ctx->use_aqo = entry->use_aqo;
		ctx->auto_tuning = entry->auto_tuning;
	}
	LWLockRelease(&aqo_state->queries_lock);
	return found;
}

/*
 * Update AQO preferences for a given queryid value.
 * if incoming param is null - leave it unchanged.
 * if forced is false, do nothing if query with such ID isn't exists yet.
 * Return true if operation have done some changes.
 */
Datum
aqo_queries_update(PG_FUNCTION_ARGS)
{
	QueriesEntry   *entry;
	uint64			queryid = PG_GETARG_INT64(AQ_QUERYID);
	bool			found;

	if (queryid == 0)
		/* Do nothing for default feature space */
		PG_RETURN_BOOL(false);

	LWLockAcquire(&aqo_state->queries_lock, LW_EXCLUSIVE);
	entry = (QueriesEntry *) hash_search(queries_htab, &queryid, HASH_FIND,
										 &found);

	if (!PG_ARGISNULL(AQ_FS))
		entry->fs = PG_GETARG_INT64(AQ_FS);
	if (!PG_ARGISNULL(AQ_LEARN_AQO))
		entry->learn_aqo = PG_GETARG_BOOL(AQ_LEARN_AQO);
	if (!PG_ARGISNULL(AQ_USE_AQO))
		entry->use_aqo = PG_GETARG_BOOL(AQ_USE_AQO);
	if (!PG_ARGISNULL(AQ_AUTO_TUNING))
		entry->auto_tuning = PG_GETARG_BOOL(AQ_AUTO_TUNING);

	/* Remove the class from cache of deactivated queries */
	hash_search(deactivated_queries, &queryid, HASH_REMOVE, NULL);

	LWLockRelease(&aqo_state->queries_lock);
	PG_RETURN_BOOL(true);
}

Datum
aqo_reset(PG_FUNCTION_ARGS)
{
	long counter = 0;

	counter += aqo_stat_reset();
	counter += aqo_qtexts_reset();
	counter += aqo_data_reset();
	counter += aqo_queries_reset();
	PG_RETURN_INT64(counter);
}

#include "utils/syscache.h"

/*
 * Scan aqo_queries. For each FS lookup aqo_data records: detect a record, where
 * list of oids links to deleted tables.
 * If
 *
 * Scan aqo_data hash table. Detect a record, where list of oids links to
 * deleted tables. If gentle is TRUE, remove this record only. Another case,
 * remove all records with the same (not default) fs from aqo_data.
 * Scan aqo_queries. If no one record in aqo_data exists for this fs - remove
 * the record from aqo_queries, aqo_query_stat and aqo_query_texts.
 */
static void
cleanup_aqo_database(bool gentle, int *fs_num, int *fss_num)
{
	HASH_SEQ_STATUS	hash_seq;
	QueriesEntry   *entry;

	/* Call it because we might touch DSA segments during the cleanup */
	dsa_init();

	*fs_num = 0;
	*fss_num = 0;

	/*
	 * It's a long haul. So, make seq scan without any lock. It is possible
	 * because only this operation can delete data from hash table.
	 */
	hash_seq_init(&hash_seq, queries_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		HASH_SEQ_STATUS	hash_seq2;
		DataEntry	   *dentry;
		List		   *junk_fss = NIL;
		List		   *actual_fss = NIL;
		ListCell	   *lc;

		/* Scan aqo_data for any junk records related to this FS */
		hash_seq_init(&hash_seq2, data_htab);
		while ((dentry = hash_seq_search(&hash_seq2)) != NULL)
		{
			char *ptr;

			if (entry->fs != dentry->key.fs)
				/* Another FS */
				continue;

			LWLockAcquire(&aqo_state->data_lock, LW_SHARED);

			Assert(DsaPointerIsValid(dentry->data_dp));
			ptr = dsa_get_address(data_dsa, dentry->data_dp);

			ptr += sizeof(data_key);
			ptr += sizeof(double) * dentry->rows * dentry->cols;
			ptr += sizeof(double) * 2 * dentry->rows;

			if (dentry->nrels > 0)
			{
				int			i;

				/* Check each OID to be existed. */
				for(i = 0; i < dentry->nrels; i++)
				{
					Oid reloid = ObjectIdGetDatum(*(Oid *)ptr);

					if (!SearchSysCacheExists1(RELOID, reloid))
						/* Remember this value */
						junk_fss = list_append_unique_int(junk_fss,
														  dentry->key.fss);
					else
						actual_fss = list_append_unique_int(actual_fss,
															dentry->key.fss);

					ptr += sizeof(Oid);
				}
			}
			else
			{
				/*
				 * Impossible case. We don't use AQO for so simple or synthetic
				 * data. Just detect errors in this logic.
				 */
				ereport(PANIC,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("AQO detected incorrect behaviour: fs="
						 UINT64_FORMAT" fss=%d",
						dentry->key.fs, (int32) dentry->key.fss)));
			}

			LWLockRelease(&aqo_state->data_lock);
		}

		/*
		 * In forced mode remove all child FSSes even some of them are still
		 * link to existed tables.
		 */
		if (junk_fss != NIL && !gentle)
			junk_fss = list_concat(junk_fss, actual_fss);

		/* Remove junk records from aqo_data */
		foreach(lc, junk_fss)
		{
			data_key	key = {.fs = entry->fs, .fss = lfirst_int(lc)};
			(*fss_num) += (int) _aqo_data_remove(&key);
		}

		/*
		 * If no one live FSS exists, remove the class totally. Don't touch
		 * default query class.
		 */
		if (entry->fs != 0 && (actual_fss == NIL || (junk_fss != NIL && !gentle)))
		{
			/* Query Stat */
			_aqo_stat_remove(entry->queryid);

			/* Query text */
			_aqo_qtexts_remove(entry->queryid);

			/* Query class preferences */
			(*fs_num) += (int) _aqo_queries_remove(entry->queryid);
		}
	}

	/*
	 * The best place to flush updated AQO storage: calling the routine, user
	 * realizes how heavy it is.
	 */
	aqo_stat_flush();
	aqo_data_flush();
	aqo_qtexts_flush();
	aqo_queries_flush();
}

Datum
aqo_cleanup(PG_FUNCTION_ARGS)
{
	int					fs_num;
	int					fss_num;
	TupleDesc			tupDesc;
	HeapTuple			tuple;
	Datum				result;
	Datum				values[2];
	bool				nulls[2] = {0, 0};

	if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	Assert(tupDesc->natts == 2);

	/*
	 * Make forced cleanup: if at least one fss isn't actual, remove parent FS
	 * and all its FSSes.
	 * Main idea of such behaviour here is, if a table was deleted, we have
	 * little chance to use this class in future. Only one use case here can be
	 * a reason: to use it as a base for search data in a set of neighbours.
	 * But, invent another UI function for such logic.
	 */
	cleanup_aqo_database(false, &fs_num, &fss_num);

	values[0] = Int32GetDatum(fs_num);
	values[1] = Int32GetDatum(fss_num);
	tuple = heap_form_tuple(tupDesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

/*
 * XXX: Maybe to allow usage of NULL value to make a reset?
 */
Datum
aqo_drop_class(PG_FUNCTION_ARGS)
{
	uint64			queryid = PG_GETARG_INT64(0);
	bool			found;
	QueriesEntry   *entry;
	uint64			fs;
	long			cnt;

	if (queryid == 0)
		elog(ERROR, "[AQO] Cannot remove basic class "INT64_FORMAT".",
			 (int64) queryid);

	/* Extract FS value for the queryid */
	LWLockAcquire(&aqo_state->queries_lock, LW_SHARED);
	entry = (QueriesEntry *) hash_search(queries_htab, &queryid, HASH_FIND,
										 &found);
	if (!found)
		elog(ERROR, "[AQO] Nothing to remove for the class "INT64_FORMAT".",
			 (int64) queryid);

	fs = entry->fs;
	LWLockRelease(&aqo_state->queries_lock);

	if (fs == 0)
		elog(ERROR, "[AQO] Cannot remove class "INT64_FORMAT" with default FS.",
			 (int64) queryid);
	if (fs != queryid)
		elog(WARNING,
			 "[AQO] Removing query class has non-generic feature space value: "
			 "id = "INT64_FORMAT", fs = "UINT64_FORMAT".", (int64) queryid, fs);

	/* Now, remove all data related to the class */
	_aqo_queries_remove(queryid);
	_aqo_stat_remove(queryid);
	_aqo_qtexts_remove(queryid);
	cnt = _aqo_data_clean(fs);

	/* Immediately save changes to permanent storage. */
	aqo_stat_flush();
	aqo_data_flush();
	aqo_qtexts_flush();
	aqo_queries_flush();

	PG_RETURN_INT32(cnt);
}

typedef enum {
	AQE_NN = 0, AQE_QUERYID, AQE_FS, AQE_CERROR, AQE_NEXECS, AQE_TOTAL_NCOLS
} ce_output_order;

/*
 * Show cardinality error gathered on last execution.
 * Skip entries with empty stat slots. XXX: is it possible?
 */
Datum
aqo_cardinality_error(PG_FUNCTION_ARGS)
{
	bool				controlled = PG_GETARG_BOOL(0);
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupDesc;
	MemoryContext		per_query_ctx;
	MemoryContext		oldcontext;
	Tuplestorestate	   *tupstore;
	Datum				values[AQE_TOTAL_NCOLS];
	bool				nulls[AQE_TOTAL_NCOLS];
	HASH_SEQ_STATUS		hash_seq;
	QueriesEntry	   *qentry;
	StatEntry		   *sentry;
	int					counter = 0;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	Assert(tupDesc->natts == AQE_TOTAL_NCOLS);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupDesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(&aqo_state->queries_lock, LW_SHARED);
	LWLockAcquire(&aqo_state->stat_lock, LW_SHARED);

	hash_seq_init(&hash_seq, queries_htab);
	while ((qentry = hash_seq_search(&hash_seq)) != NULL)
	{
		bool	found;
		double *ce;
		int64	nexecs;
		int		nvals;

		memset(nulls, 0, AQE_TOTAL_NCOLS * sizeof(nulls[0]));

		sentry = (StatEntry *) hash_search(stat_htab, &qentry->queryid,
										   HASH_FIND, &found);
		if (!found)
			/* Statistics not found by some reason. Just go further */
			continue;

		nvals = controlled ? sentry->cur_stat_slot_aqo : sentry->cur_stat_slot;
		if (nvals == 0)
			/* No one stat slot filled */
			continue;

		nexecs = controlled ? sentry->execs_with_aqo : sentry->execs_without_aqo;
		ce = controlled ? sentry->est_error_aqo : sentry->est_error;

		values[AQE_NN] = Int32GetDatum(++counter);
		values[AQE_QUERYID] = Int64GetDatum(qentry->queryid);
		values[AQE_FS] = Int64GetDatum(qentry->fs);
		values[AQE_NEXECS] = Int64GetDatum(nexecs);
		values[AQE_CERROR] = Float8GetDatum(ce[nvals - 1]);
		tuplestore_putvalues(tupstore, tupDesc, values, nulls);
	}

	LWLockRelease(&aqo_state->stat_lock);
	LWLockRelease(&aqo_state->queries_lock);

	tuplestore_donestoring(tupstore);
	return (Datum) 0;
}

typedef enum {
	ET_NN = 0, ET_QUERYID, ET_FS, ET_EXECTIME, ET_NEXECS, ET_TOTAL_NCOLS
} et_output_order;

/*
 * XXX: maybe to merge with aqo_cardinality_error ?
 * XXX: Do we really want sequental number ?
 */
Datum
aqo_execution_time(PG_FUNCTION_ARGS)
{
	bool				controlled = PG_GETARG_BOOL(0);
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupDesc;
	MemoryContext		per_query_ctx;
	MemoryContext		oldcontext;
	Tuplestorestate	   *tupstore;
	Datum				values[AQE_TOTAL_NCOLS];
	bool				nulls[AQE_TOTAL_NCOLS];
	HASH_SEQ_STATUS		hash_seq;
	QueriesEntry	   *qentry;
	StatEntry		   *sentry;
	int					counter = 0;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	Assert(tupDesc->natts == ET_TOTAL_NCOLS);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupDesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(&aqo_state->queries_lock, LW_SHARED);
	LWLockAcquire(&aqo_state->stat_lock, LW_SHARED);

	hash_seq_init(&hash_seq, queries_htab);
	while ((qentry = hash_seq_search(&hash_seq)) != NULL)
	{
		bool	found;
		double *et;
		int64	nexecs;
		int		nvals;
		double	tm = 0;

		memset(nulls, 0, ET_TOTAL_NCOLS * sizeof(nulls[0]));

		sentry = (StatEntry *) hash_search(stat_htab, &qentry->queryid,
										   HASH_FIND, &found);
		if (!found)
			/* Statistics not found by some reason. Just go further */
			continue;

		nvals = controlled ? sentry->cur_stat_slot_aqo : sentry->cur_stat_slot;
		if (nvals == 0)
			/* No one stat slot filled */
			continue;

		nexecs = controlled ? sentry->execs_with_aqo : sentry->execs_without_aqo;
		et = controlled ? sentry->exec_time_aqo : sentry->exec_time;

		if (!controlled)
		{
			int i;
			/* Calculate average execution time */
			for (i = 0; i < nvals; i++)
				tm += et[i];
			tm /= nvals;
		}
		else
			tm = et[nvals - 1];

		values[ET_NN] = Int32GetDatum(++counter);
		values[ET_QUERYID] = Int64GetDatum(qentry->queryid);
		values[ET_FS] = Int64GetDatum(qentry->fs);
		values[ET_NEXECS] = Int64GetDatum(nexecs);
		values[ET_EXECTIME] = Float8GetDatum(tm);
		tuplestore_putvalues(tupstore, tupDesc, values, nulls);
	}

	LWLockRelease(&aqo_state->stat_lock);
	LWLockRelease(&aqo_state->queries_lock);

	tuplestore_donestoring(tupstore);
	return (Datum) 0;
}
