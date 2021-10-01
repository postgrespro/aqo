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
 * Copyright (c) 2016-2021, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/storage.c
 *
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"

#include "aqo.h"
#include "preprocessing.h"


HTAB *deactivated_queries = NULL;

static ArrayType *form_matrix(double **matrix, int nrows, int ncols);
static void deform_matrix(Datum datum, double **matrix);

static ArrayType *form_vector(double *vector, int nrows);
static void deform_vector(Datum datum, double *vector, int *nelems);

#define FormVectorSz(v_name)			(form_vector((v_name), (v_name ## _size)))
#define DeformVectorSz(datum, v_name)	(deform_vector((datum), (v_name), &(v_name ## _size)))


static bool my_simple_heap_update(Relation relation,
								  ItemPointer otid,
								  HeapTuple tup,
								  bool *update_indexes);


/*
 * Returns whether the query with given hash is in aqo_queries.
 * If yes, returns the content of the first line with given hash.
 *
 * Use dirty snapshot to see all (include in-progess) data. We want to prevent
 * wait in the XactLockTableWait routine.
 */
bool
find_query(int qhash, Datum *search_values, bool *search_nulls)
{
	RangeVar   *rv;
	Relation	hrel;
	Relation	irel;
	HeapTuple	tuple;
	TupleTableSlot *slot;
	bool shouldFree;
	Oid			reloid;
	IndexScanDesc scan;
	ScanKeyData key;
	SnapshotData snap;
	bool		find_ok = false;

	reloid = RelnameGetRelid("aqo_queries_query_hash_idx");
	if (!OidIsValid(reloid))
	{
		disable_aqo_for_query();
		return false;
	}

	rv = makeRangeVar("public", "aqo_queries", -1);
	hrel = table_openrv(rv,  AccessShareLock);
	irel = index_open(reloid,  AccessShareLock);

	InitDirtySnapshot(snap);
	scan = index_beginscan(hrel, irel, &snap, 1, 0);
	ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(qhash));

	index_rescan(scan, &key, 1, NULL, 0);
	slot = MakeSingleTupleTableSlot(hrel->rd_att, &TTSOpsBufferHeapTuple);
	find_ok = index_getnext_slot(scan, ForwardScanDirection, slot);

	if (find_ok)
	{
		tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
		Assert(shouldFree != true);
		heap_deform_tuple(tuple, hrel->rd_att, search_values, search_nulls);
	}

	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	index_close(irel,  AccessShareLock);
	table_close(hrel,  AccessShareLock);

	return find_ok;
}

/*
 * Update query status in intelligent mode.
 *
 * Do it gently: to prevent possible deadlocks, revert this update if any
 * concurrent transaction is doing it.
 *
 * Such logic is possible, because this update is performed by AQO itself. It is
 * not break any learning logic besides possible additional learning iterations.
 */
bool
update_query(int qhash, int fhash,
			 bool learn_aqo, bool use_aqo, bool auto_tuning)
{
	RangeVar   *rv;
	Relation	hrel;
	Relation	irel;
	TupleTableSlot *slot;
	HeapTuple	tuple,
				nw_tuple;
	Datum		values[5];
	bool		isnull[5] = { false, false, false, false, false };
	bool		replace[5] = { false, true, true, true, true };
	bool		shouldFree;
	bool		result = true;
	bool		update_indexes;
	Oid			reloid;
	IndexScanDesc scan;
	ScanKeyData key;
	SnapshotData snap;

	/* Couldn't allow to write if xact must be read-only. */
	if (XactReadOnly)
		return false;

	reloid = RelnameGetRelid("aqo_queries_query_hash_idx");
	if (!OidIsValid(reloid))
	{
		disable_aqo_for_query();
		return false;
	}

	rv = makeRangeVar("public", "aqo_queries", -1);
	hrel = table_openrv(rv, RowExclusiveLock);
	irel = index_open(reloid, RowExclusiveLock);

	/*
	 * Start an index scan. Use dirty snapshot to check concurrent updates that
	 * can be made before, but still not visible.
	 */
	InitDirtySnapshot(snap);
	scan = index_beginscan(hrel, irel, &snap, 1, 0);
	ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(qhash));

	index_rescan(scan, &key, 1, NULL, 0);
	slot = MakeSingleTupleTableSlot(hrel->rd_att, &TTSOpsBufferHeapTuple);

	values[0] = Int32GetDatum(qhash);
	values[1] = BoolGetDatum(learn_aqo);
	values[2] = BoolGetDatum(use_aqo);
	values[3] = Int32GetDatum(fhash);
	values[4] = BoolGetDatum(auto_tuning);

	if (!index_getnext_slot(scan, ForwardScanDirection, slot))
	{
		/* New tuple for the ML knowledge base */
		tuple = heap_form_tuple(RelationGetDescr(hrel), values, isnull);
		simple_heap_insert(hrel, tuple);
		my_index_insert(irel, values, isnull, &(tuple->t_self),
														hrel, UNIQUE_CHECK_YES);
	}
	else if (!TransactionIdIsValid(snap.xmin) &&
			 !TransactionIdIsValid(snap.xmax))
	{
		/*
		 * Update existed data. No one concurrent transaction doesn't update this
		 * right now.
		 */
		tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
		Assert(shouldFree != true);
		nw_tuple = heap_modify_tuple(tuple, hrel->rd_att, values, isnull, replace);

		if (my_simple_heap_update(hrel, &(nw_tuple->t_self), nw_tuple,
															&update_indexes))
		{
			if (update_indexes)
				my_index_insert(irel, values, isnull,
								&(nw_tuple->t_self),
								hrel, UNIQUE_CHECK_YES);
			result = true;
		}
		else
		{
			/*
			 * Ooops, somebody concurrently updated the tuple. It is possible
			 * only in the case of changes made by third-party code.
			 */
			elog(ERROR, "AQO feature space data for signature (%d, %d) concurrently"
						" updated by a stranger backend.",
						qhash, fhash);
			result = false;
		}
	}
	else
	{
		/*
		 * Concurrent update was made. To prevent deadlocks refuse to update.
		 */
		result = false;
	}

	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	index_close(irel, RowExclusiveLock);
	table_close(hrel, RowExclusiveLock);

	CommandCounterIncrement();
	return result;
}

/*
 * Creates entry for new query in aqo_query_texts table with given fields.
 * Returns false if the operation failed, true otherwise.
 */
bool
add_query_text(int qhash, const char *query_string)
{
	RangeVar   *rv;
	Relation	hrel;
	Relation	irel;
	HeapTuple	tuple;
	Datum		values[2];
	bool		isnull[2] = {false, false};
	Oid			reloid;

	values[0] = Int32GetDatum(qhash);
	values[1] = CStringGetTextDatum(query_string);

	/* Couldn't allow to write if xact must be read-only. */
	if (XactReadOnly)
		return false;

	reloid = RelnameGetRelid("aqo_query_texts_query_hash_idx");
	if (!OidIsValid(reloid))
	{
		disable_aqo_for_query();
		return false;
	}

	rv = makeRangeVar("public", "aqo_query_texts", -1);
	hrel = table_openrv(rv, RowExclusiveLock);
	irel = index_open(reloid, RowExclusiveLock);
	tuple = heap_form_tuple(RelationGetDescr(hrel), values, isnull);

	simple_heap_insert(hrel, tuple);
	my_index_insert(irel, values, isnull, &(tuple->t_self), hrel,
															UNIQUE_CHECK_YES);

	index_close(irel, RowExclusiveLock);
	table_close(hrel, RowExclusiveLock);

	CommandCounterIncrement();
	return true;
}


static ArrayType *
form_oids_vector(List *relids)
{
	Datum	   *oids;
	ArrayType  *array;
	ListCell   *lc;
	int			i = 0;

	if (relids == NIL)
		return NULL;

	oids = (Datum *) palloc(list_length(relids) * sizeof(Datum));

	foreach(lc, relids)
	{
		Oid relid = lfirst_oid(lc);

		oids[i++] = ObjectIdGetDatum(relid);
	}

	Assert(i == list_length(relids));
	array = construct_array(oids, i, OIDOID, sizeof(Oid), true, TYPALIGN_INT);
	pfree(oids);
	return array;
}

static List *
deform_oids_vector(Datum datum)
{
	ArrayType	*array = DatumGetArrayTypePCopy(PG_DETOAST_DATUM(datum));
	Datum		*values;
	int			i;
	int			nelems = 0;
	List		*relids = NIL;

	deconstruct_array(array,
					  OIDOID, sizeof(Oid), true, TYPALIGN_INT,
					  &values, NULL, &nelems);
	for (i = 0; i < nelems; ++i)
		relids = lappend_oid(relids, DatumGetObjectId(values[i]));

	pfree(values);
	pfree(array);
	return relids;
}

/*
 * Loads feature subspace (fss) from table aqo_data into memory.
 * The last column of the returned matrix is for target values of objects.
 * Returns false if the operation failed, true otherwise.
 *
 * 'fss_hash' is the hash of feature subspace which is supposed to be loaded
 * 'ncols' is the number of clauses in the feature subspace
 * 'matrix' is an allocated memory for matrix with the size of aqo_K rows
 *			and nhashes columns
 * 'targets' is an allocated memory with size aqo_K for target values
 *			of the objects
 * 'rows' is the pointer in which the function stores actual number of
 *			objects in the given feature space
 */
bool
load_fss(int fhash, int fss_hash,
		 int ncols, double **matrix, double *targets, int *rows,
		 List **relids)
{
	RangeVar	*rv;
	Relation	hrel;
	Relation	irel;
	HeapTuple	tuple;
	TupleTableSlot *slot;
	bool		shouldFree;
	bool		find_ok = false;
	Oid			reloid;
	IndexScanDesc scan;
	ScanKeyData	key[2];
	Datum		values[6];
	bool		isnull[6];
	bool		success = true;

	reloid = RelnameGetRelid("aqo_fss_access_idx");
	if (!OidIsValid(reloid))
	{
		disable_aqo_for_query();
		return false;
	}

	rv = makeRangeVar("public", "aqo_data", -1);
	hrel = table_openrv(rv, AccessShareLock);
	irel = index_open(reloid, AccessShareLock);
	scan = index_beginscan(hrel, irel, SnapshotSelf, 2, 0);

	ScanKeyInit(&key[0], 1, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(fhash));
	ScanKeyInit(&key[1], 2, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(fss_hash));
	index_rescan(scan, key, 2, NULL, 0);

	slot = MakeSingleTupleTableSlot(hrel->rd_att, &TTSOpsBufferHeapTuple);
	find_ok = index_getnext_slot(scan, ForwardScanDirection, slot);

	if (matrix == NULL && targets == NULL && rows == NULL)
	{
		/* Just check availability */
		success = find_ok;
	}
	else if (find_ok)
	{
		tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
		Assert(shouldFree != true);
		heap_deform_tuple(tuple, hrel->rd_att, values, isnull);

		if (DatumGetInt32(values[2]) == ncols)
		{
			if (ncols > 0)
				/*
				 * The case than an object has not any filters and selectivities
				 */
				deform_matrix(values[3], matrix);

			deform_vector(values[4], targets, rows);

			if (relids != NULL)
				*relids = deform_oids_vector(values[5]);
		}
		else
			elog(ERROR, "unexpected number of features for hash (%d, %d):\
						   expected %d features, obtained %d",
						   fhash, fss_hash, ncols, DatumGetInt32(values[2]));
	}
	else
		success = false;

	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	index_close(irel, AccessShareLock);
	table_close(hrel, AccessShareLock);

	return success;
}

/*
 * Updates the specified line in the specified feature subspace.
 * Returns false if the operation failed, true otherwise.
 *
 * 'fss_hash' specifies the feature subspace 'nrows' x 'ncols' is the shape
 * of 'matrix' 'targets' is vector of size 'nrows'
 *
 * Necessary to prevent waiting for another transaction to commit in index
 * insertion or heap update.
 *
 * Caller guaranteed that no one AQO process insert or update this data row.
 */
bool
update_fss(int fhash, int fsshash, int nrows, int ncols,
		   double **matrix, double *targets, List *relids)
{
	RangeVar   *rv;
	Relation	hrel;
	Relation	irel;
	SnapshotData snap;
	TupleTableSlot *slot;
	TupleDesc	tupDesc;
	HeapTuple	tuple,
				nw_tuple;
	Datum		values[6];
	bool		isnull[6] = { false, false, false, false, false, false };
	bool		replace[6] = { false, false, false, true, true, false };
	bool		shouldFree;
	bool		find_ok = false;
	bool		update_indexes;
	Oid			reloid;
	IndexScanDesc scan;
	ScanKeyData	key[2];
	bool result = true;

	/* Couldn't allow to write if xact must be read-only. */
	if (XactReadOnly)
		return false;

	reloid = RelnameGetRelid("aqo_fss_access_idx");
	if (!OidIsValid(reloid))
	{
		disable_aqo_for_query();
		return false;
	}

	rv = makeRangeVar("public", "aqo_data", -1);
	hrel = table_openrv(rv, RowExclusiveLock);
	irel = index_open(reloid, RowExclusiveLock);
	tupDesc = RelationGetDescr(hrel);

	InitDirtySnapshot(snap);
	scan = index_beginscan(hrel, irel, &snap, 2, 0);

	ScanKeyInit(&key[0], 1, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(fhash));
	ScanKeyInit(&key[1], 2, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(fsshash));

	index_rescan(scan, key, 2, NULL, 0);

	slot = MakeSingleTupleTableSlot(tupDesc, &TTSOpsBufferHeapTuple);
	find_ok = index_getnext_slot(scan, ForwardScanDirection, slot);

	if (!find_ok)
	{
		values[0] = Int32GetDatum(fhash);
		values[1] = Int32GetDatum(fsshash);
		values[2] = Int32GetDatum(ncols);

		if (ncols > 0)
			values[3] = PointerGetDatum(form_matrix(matrix, nrows, ncols));
		else
			isnull[3] = true;

		values[4] = PointerGetDatum(form_vector(targets, nrows));

		/* Form array of relids. Only once. */
		values[5] = PointerGetDatum(form_oids_vector(relids));
		if ((void *) values[5] == NULL)
			isnull[5] = true;
		tuple = heap_form_tuple(tupDesc, values, isnull);

		/*
		 * Don't use PG_TRY() section because of dirty snapshot and caller atomic
		 * prerequisities guarantees to us that no one concurrent insertion can
		 * exists.
		 */
		simple_heap_insert(hrel, tuple);
		my_index_insert(irel, values, isnull, &(tuple->t_self),
														hrel, UNIQUE_CHECK_YES);
	}
	else if (!TransactionIdIsValid(snap.xmin) && !TransactionIdIsValid(snap.xmax))
	{
		tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
		Assert(shouldFree != true);
		heap_deform_tuple(tuple, hrel->rd_att, values, isnull);

		if (ncols > 0)
			values[3] = PointerGetDatum(form_matrix(matrix, nrows, ncols));
		else
			isnull[3] = true;

		values[4] = PointerGetDatum(form_vector(targets, nrows));
		nw_tuple = heap_modify_tuple(tuple, tupDesc,
									 values, isnull, replace);
		if (my_simple_heap_update(hrel, &(nw_tuple->t_self), nw_tuple,
															&update_indexes))
		{
			if (update_indexes)
				my_index_insert(irel, values, isnull,
								&(nw_tuple->t_self),
								hrel, UNIQUE_CHECK_YES);
			result = true;
		}
		else
		{
			/*
			 * Ooops, somebody concurrently updated the tuple. It is possible
			 * only in the case of changes made by third-party code.
			 */
			elog(ERROR, "AQO data piece (%d %d) concurrently updated"
				 " by a stranger backend.",
				 fhash, fsshash);
			result = false;
		}
	}
	else
	{
		/*
		 * Concurrent update was made. To prevent deadlocks refuse to update.
		 */
		result = false;
	}

	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	index_close(irel, RowExclusiveLock);
	table_close(hrel, RowExclusiveLock);

	CommandCounterIncrement();
	return result;
}

/*
 * Returns QueryStat for the given query_hash. Returns empty QueryStat if
 * no statistics is stored for the given query_hash in table aqo_query_stat.
 * Returns NULL and executes disable_aqo_for_query if aqo_query_stat
 * is not found.
 */
QueryStat *
get_aqo_stat(int qhash)
{
	RangeVar   *rv;
	Relation	hrel;
	Relation	irel;
	TupleTableSlot *slot;
	Oid			reloid;
	IndexScanDesc scan;
	ScanKeyData key;
	QueryStat  *stat = palloc_query_stat();
	bool		shouldFree;

	reloid = RelnameGetRelid("aqo_query_stat_idx");
	if (!OidIsValid(reloid))
	{
		disable_aqo_for_query();
		return NULL;
	}

	rv = makeRangeVar("public", "aqo_query_stat", -1);
	hrel = table_openrv(rv, AccessShareLock);
	irel = index_open(reloid, AccessShareLock);

	scan = index_beginscan(hrel, irel, SnapshotSelf, 1, 0);
	ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(qhash));
	index_rescan(scan, &key, 1, NULL, 0);
	slot = MakeSingleTupleTableSlot(hrel->rd_att, &TTSOpsBufferHeapTuple);

	if (index_getnext_slot(scan, ForwardScanDirection, slot))
	{
		HeapTuple	tuple;
		Datum		values[9];
		bool		nulls[9];

		tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
		Assert(shouldFree != true);
		heap_deform_tuple(tuple, hrel->rd_att, values, nulls);

		DeformVectorSz(values[1], stat->execution_time_with_aqo);
		DeformVectorSz(values[2], stat->execution_time_without_aqo);
		DeformVectorSz(values[3], stat->planning_time_with_aqo);
		DeformVectorSz(values[4], stat->planning_time_without_aqo);
		DeformVectorSz(values[5], stat->cardinality_error_with_aqo);
		DeformVectorSz(values[6], stat->cardinality_error_without_aqo);

		stat->executions_with_aqo = DatumGetInt64(values[7]);
		stat->executions_without_aqo = DatumGetInt64(values[8]);
	}

	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	index_close(irel, AccessShareLock);
	table_close(hrel, AccessShareLock);
	return stat;
}

/*
 * Saves given QueryStat for the given query_hash.
 * Executes disable_aqo_for_query if aqo_query_stat is not found.
 */
void
update_aqo_stat(int qhash, QueryStat *stat)
{
	RangeVar   *rv;
	Relation	hrel;
	Relation	irel;
	SnapshotData snap;
	TupleTableSlot *slot;
	TupleDesc	tupDesc;
	HeapTuple	tuple,
				nw_tuple;
	Datum		values[9];
	bool		isnull[9] = { false, false, false,
							  false, false, false,
							  false, false, false };
	bool		replace[9] = { false, true, true,
							    true, true, true,
								true, true, true };
	bool		shouldFree;
	bool		update_indexes;
	Oid			reloid;
	IndexScanDesc scan;
	ScanKeyData	key;

	/* Couldn't allow to write if xact must be read-only. */
	if (XactReadOnly)
		return;

	reloid = RelnameGetRelid("aqo_query_stat_idx");
	if (!OidIsValid(reloid))
	{
		disable_aqo_for_query();
		return;
	}

	rv = makeRangeVar("public", "aqo_query_stat", -1);
	hrel = table_openrv(rv, RowExclusiveLock);
	irel = index_open(reloid, RowExclusiveLock);
	tupDesc = RelationGetDescr(hrel);

	InitDirtySnapshot(snap);
	scan = index_beginscan(hrel, irel, &snap, 1, 0);
	ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(qhash));
	index_rescan(scan, &key, 1, NULL, 0);
	slot = MakeSingleTupleTableSlot(hrel->rd_att, &TTSOpsBufferHeapTuple);

	/*values[0] will be initialized later */
	values[1] = PointerGetDatum(FormVectorSz(stat->execution_time_with_aqo));
	values[2] = PointerGetDatum(FormVectorSz(stat->execution_time_without_aqo));
	values[3] = PointerGetDatum(FormVectorSz(stat->planning_time_with_aqo));
	values[4] = PointerGetDatum(FormVectorSz(stat->planning_time_without_aqo));
	values[5] = PointerGetDatum(FormVectorSz(stat->cardinality_error_with_aqo));
	values[6] = PointerGetDatum(FormVectorSz(stat->cardinality_error_without_aqo));

	values[7] = Int64GetDatum(stat->executions_with_aqo);
	values[8] = Int64GetDatum(stat->executions_without_aqo);

	if (!index_getnext_slot(scan, ForwardScanDirection, slot))
	{
		/* Such signature (hash) doesn't yet exist in the ML knowledge base. */
		values[0] = Int32GetDatum(qhash);
		tuple = heap_form_tuple(tupDesc, values, isnull);
		simple_heap_insert(hrel, tuple);
		my_index_insert(irel, values, isnull, &(tuple->t_self),
														hrel, UNIQUE_CHECK_YES);
	}
	else if (!TransactionIdIsValid(snap.xmin) && !TransactionIdIsValid(snap.xmax))
	{
		/* Need to update ML data row and no one backend concurrently doing it. */
		tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
		Assert(shouldFree != true);
		values[0] = heap_getattr(tuple, 1, tupDesc, &isnull[0]);
		nw_tuple = heap_modify_tuple(tuple, tupDesc, values, isnull, replace);
		if (my_simple_heap_update(hrel, &(nw_tuple->t_self), nw_tuple,
															&update_indexes))
		{
			/* NOTE: insert index tuple iff heap update succeeded! */
			if (update_indexes)
				my_index_insert(irel, values, isnull,
								&(nw_tuple->t_self),
								hrel, UNIQUE_CHECK_YES);
		}
		else
		{
			/*
			 * Ooops, somebody concurrently updated the tuple. It is possible
			 * only in the case of changes made by third-party code.
			 */
			elog(ERROR, "AQO statistic data for query signature %d concurrently"
						" updated by a stranger backend.",
				 qhash);
		}
	}
	else
	{
		/*
		 * Concurrent update was made. To prevent deadlocks refuse to update.
		 */
	}

	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	index_close(irel, RowExclusiveLock);
	table_close(hrel, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * Expands matrix from storage into simple C-array.
 */
void
deform_matrix(Datum datum, double **matrix)
{
	ArrayType  *array = DatumGetArrayTypePCopy(PG_DETOAST_DATUM(datum));
	int			nelems;
	Datum	   *values;
	int			rows;
	int			cols;
	int			i,
				j;

	deconstruct_array(array,
					  FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd',
					  &values, NULL, &nelems);
	if (nelems != 0)
	{
		rows = ARR_DIMS(array)[0];
		cols = ARR_DIMS(array)[1];
		for (i = 0; i < rows; ++i)
			for (j = 0; j < cols; ++j)
				matrix[i][j] = DatumGetFloat8(values[i * cols + j]);
	}
	pfree(values);
	pfree(array);
}

/*
 * Expands vector from storage into simple C-array.
 * Also returns its number of elements.
 */
void
deform_vector(Datum datum, double *vector, int *nelems)
{
	ArrayType  *array = DatumGetArrayTypePCopy(PG_DETOAST_DATUM(datum));
	Datum	   *values;
	int			i;

	deconstruct_array(array,
					  FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd',
					  &values, NULL, nelems);
	for (i = 0; i < *nelems; ++i)
		vector[i] = DatumGetFloat8(values[i]);
	pfree(values);
	pfree(array);
}

/*
 * Forms ArrayType object for storage from simple C-array matrix.
 */
ArrayType *
form_matrix(double **matrix, int nrows, int ncols)
{
	Datum	   *elems;
	ArrayType  *array;
	int			dims[2];
	int			lbs[2];
	int			i,
				j;

	dims[0] = nrows;
	dims[1] = ncols;
	lbs[0] = lbs[1] = 1;
	elems = palloc(sizeof(*elems) * nrows * ncols);
	for (i = 0; i < nrows; ++i)
		for (j = 0; j < ncols; ++j)
			elems[i * ncols + j] = Float8GetDatum(matrix[i][j]);

	array = construct_md_array(elems, NULL, 2, dims, lbs,
							   FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd');
	pfree(elems);
	return array;
}

/*
 * Forms ArrayType object for storage from simple C-array vector.
 */
ArrayType *
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
	pfree(elems);
	return array;
}

/*
 * Returns true if updated successfully, false if updated concurrently by
 * another session, error otherwise.
 */
static bool
my_simple_heap_update(Relation relation, ItemPointer otid, HeapTuple tup,
					  bool *update_indexes)
{
	TM_Result result;
	TM_FailureData hufd;
	LockTupleMode lockmode;

	Assert(update_indexes != NULL);
	result = heap_update(relation, otid, tup,
						 GetCurrentCommandId(true), InvalidSnapshot,
						 true /* wait for commit */ ,
						 &hufd, &lockmode);
	switch (result)
	{
		case TM_SelfModified:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case TM_Ok:
			/* done successfully */
			if (!HeapTupleIsHeapOnly(tup))
				*update_indexes = true;
			else
				*update_indexes = false;
			return true;

		case TM_Updated:
			return false;
			break;

		case TM_BeingModified:
			return false;
			break;

		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			break;
	}
	return false;
}


/* Provides correct insert in both PostgreQL 9.6.X and 10.X.X */
bool
my_index_insert(Relation indexRelation,
				Datum *values, bool *isnull,
				ItemPointer heap_t_ctid,
				Relation heapRelation,
				IndexUniqueCheck checkUnique)
{
	/* Index must be UNIQUE to support uniqueness checks */
	Assert(checkUnique == UNIQUE_CHECK_NO ||
		   indexRelation->rd_index->indisunique);

#if PG_VERSION_NUM < 100000
	return index_insert(indexRelation, values, isnull, heap_t_ctid,
						heapRelation, checkUnique);
#elif PG_VERSION_NUM < 140000
	return index_insert(indexRelation, values, isnull, heap_t_ctid,
						heapRelation, checkUnique,
						BuildIndexInfo(indexRelation));
#else
	return index_insert(indexRelation, values, isnull, heap_t_ctid,
						heapRelation, checkUnique, false,
						BuildIndexInfo(indexRelation));
#endif
}

/* Creates a storage for hashes of deactivated queries */
void
init_deactivated_queries_storage(void)
{
	HASHCTL		hash_ctl;

	/* Create the hashtable proper */
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(int);
	hash_ctl.entrysize = sizeof(int);
	deactivated_queries = hash_create("aqo_deactivated_queries",
									  128,		/* start small and extend */
									  &hash_ctl,
									  HASH_ELEM | HASH_BLOBS);
}

/* Destroys the storage for hash of deactivated queries */
void
fini_deactivated_queries_storage(void)
{
	hash_destroy(deactivated_queries);
	deactivated_queries = NULL;
}

/* Checks whether the query with given hash is deactivated */
bool
query_is_deactivated(int query_hash)
{
	bool		found;

	hash_search(deactivated_queries, &query_hash, HASH_FIND, &found);
	return found;
}

/* Adds given query hash into the set of hashes of deactivated queries*/
void
add_deactivated_query(int query_hash)
{
	hash_search(deactivated_queries, &query_hash, HASH_ENTER, NULL);
}
