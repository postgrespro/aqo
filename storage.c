#include "aqo.h"

/*****************************************************************************
 *
 *	STORAGE INTERACTION
 *
 * This module is responsible for intercation with the storage of aqo data.
 * It does not provide information protection from concurrent updates.
 *
 *****************************************************************************/

HTAB  *deactivated_queries = NULL;

static void deform_matrix(Datum datum, double **matrix);
static void deform_vector(Datum datum, double *vector, int *nelems);
static ArrayType *form_matrix(double **matrix, int nrows, int ncols);
static ArrayType *form_vector(double *vector, int nrows);
static bool my_simple_heap_update(Relation relation,
								  ItemPointer otid,
								  HeapTuple tup);


/*
 * Returns whether the query with given hash is in aqo_queries.
 * If yes, returns the content of the first line with given hash.
 */
bool
find_query(int query_hash,
		   Datum *search_values,
		   bool *search_nulls)
{
	RangeVar   *aqo_queries_table_rv;
	Relation	aqo_queries_heap;
	HeapTuple	tuple;
	LOCKMODE	heap_lock = AccessShareLock;

	Relation	query_index_rel;
	Oid			query_index_rel_oid;
	IndexScanDesc query_index_scan;
	ScanKeyData key;

	bool		find_ok = false;

	query_index_rel_oid = RelnameGetRelid("aqo_queries_query_hash_idx");
	if (!OidIsValid(query_index_rel_oid))
	{
		disable_aqo_for_query();
		return false;
	}

	aqo_queries_table_rv = makeRangeVar("public", "aqo_queries", -1);
	aqo_queries_heap = heap_openrv(aqo_queries_table_rv, heap_lock);

	query_index_rel = index_open(query_index_rel_oid, heap_lock);
	query_index_scan = index_beginscan(
									   aqo_queries_heap,
									   query_index_rel,
									   SnapshotSelf,
									   1,
									   0
		);
	ScanKeyInit(&key,
				1,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(query_hash)
		);

	index_rescan(query_index_scan, &key, 1, NULL, 0);
	tuple = index_getnext(query_index_scan, ForwardScanDirection);

	find_ok = (tuple != NULL);

	if (find_ok)
		heap_deform_tuple(tuple, aqo_queries_heap->rd_att,
						  search_values, search_nulls);

	index_endscan(query_index_scan);
	index_close(query_index_rel, heap_lock);
	heap_close(aqo_queries_heap, heap_lock);

	return find_ok;
}

/*
 * Creates entry for new query in aqo_queries table with given fields.
 * Returns false if the operation failed, true otherwise.
 */
bool
add_query(int query_hash, bool learn_aqo, bool use_aqo,
		  int fspace_hash, bool auto_tuning)
{
	RangeVar   *aqo_queries_table_rv;
	Relation	aqo_queries_heap;
	HeapTuple	tuple;
	LOCKMODE	heap_lock = RowExclusiveLock;

	Datum		values[5];
	bool		nulls[5] = {false, false, false, false, false};

	Relation	query_index_rel;
	Oid			query_index_rel_oid;

	values[0] = Int32GetDatum(query_hash);
	values[1] = BoolGetDatum(learn_aqo);
	values[2] = BoolGetDatum(use_aqo);
	values[3] = Int32GetDatum(fspace_hash);
	values[4] = BoolGetDatum(auto_tuning);

	query_index_rel_oid = RelnameGetRelid("aqo_queries_query_hash_idx");
	if (!OidIsValid(query_index_rel_oid))
	{
		disable_aqo_for_query();
		return false;
	}
	query_index_rel = index_open(query_index_rel_oid, heap_lock);

	aqo_queries_table_rv = makeRangeVar("public", "aqo_queries", -1);
	aqo_queries_heap = heap_openrv(aqo_queries_table_rv, heap_lock);

	tuple = heap_form_tuple(RelationGetDescr(aqo_queries_heap),
							values, nulls);
	PG_TRY();
	{
		simple_heap_insert(aqo_queries_heap, tuple);
		index_insert(query_index_rel,
					 values, nulls,
					 &(tuple->t_self),
					 aqo_queries_heap,
					 UNIQUE_CHECK_YES);
	}
	PG_CATCH();
	{
		CommandCounterIncrement();
		simple_heap_delete(aqo_queries_heap, &(tuple->t_self));
	}
	PG_END_TRY();

	index_close(query_index_rel, heap_lock);
	heap_close(aqo_queries_heap, heap_lock);

	CommandCounterIncrement();

	return true;
}

bool
update_query(int query_hash, bool learn_aqo, bool use_aqo,
			 int fspace_hash, bool auto_tuning)
{
	RangeVar   *aqo_queries_table_rv;
	Relation	aqo_queries_heap;
	HeapTuple	tuple,
				nw_tuple;
	LOCKMODE	heap_lock = RowExclusiveLock;

	Relation	query_index_rel;
	Oid			query_index_rel_oid;
	IndexScanDesc query_index_scan;
	ScanKeyData key;

	Datum		values[5];
	bool		nulls[5] = {false, false, false, false, false};
	bool		do_replace[5] = {false, true, true, true, true};

	query_index_rel_oid = RelnameGetRelid("aqo_queries_query_hash_idx");
	if (!OidIsValid(query_index_rel_oid))
	{
		disable_aqo_for_query();
		return false;
	}

	aqo_queries_table_rv = makeRangeVar("public", "aqo_queries", -1);
	aqo_queries_heap = heap_openrv(aqo_queries_table_rv, heap_lock);

	query_index_rel = index_open(query_index_rel_oid, heap_lock);
	query_index_scan = index_beginscan(
									   aqo_queries_heap,
									   query_index_rel,
									   SnapshotSelf,
									   1,
									   0
		);
	ScanKeyInit(&key,
				1,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(query_hash)
		);

	index_rescan(query_index_scan, &key, 1, NULL, 0);
	tuple = index_getnext(query_index_scan, ForwardScanDirection);

	heap_deform_tuple(tuple, aqo_queries_heap->rd_att,
					  values, nulls);

	values[1] = BoolGetDatum(learn_aqo);
	values[2] = BoolGetDatum(use_aqo);
	values[3] = Int32GetDatum(fspace_hash);
	values[4] = BoolGetDatum(auto_tuning);

	nw_tuple = heap_modify_tuple(tuple, aqo_queries_heap->rd_att,
								 values, nulls, do_replace);
	if (my_simple_heap_update(aqo_queries_heap, &(nw_tuple->t_self), nw_tuple))
	{
		index_insert(query_index_rel, values, nulls, &(nw_tuple->t_self),
					 aqo_queries_heap, UNIQUE_CHECK_YES);
	}
	else
	{
		/*
		 * Ooops, somebody concurrently updated the tuple. We have to merge our
		 * changes somehow, but now we just discard ours. We don't believe in
		 * high probability of simultaneously finishing of two long, complex,
		 * and important queries, so we don't loss important data.
		 */
	}

	index_endscan(query_index_scan);
	index_close(query_index_rel, heap_lock);
	heap_close(aqo_queries_heap, heap_lock);

	CommandCounterIncrement();

	return true;
}

/*
 * Creates entry for new query in aqo_query_texts table with given fields.
 * Returns false if the operation failed, true otherwise.
 */
bool
add_query_text(int query_hash, const char *query_text)
{
	RangeVar   *aqo_query_texts_table_rv;
	Relation	aqo_query_texts_heap;
	HeapTuple	tuple;
	LOCKMODE	heap_lock = RowExclusiveLock;

	Datum		values[2];
	bool		nulls[2] = {false, false};

	Relation	query_index_rel;
	Oid			query_index_rel_oid;

	values[0] = Int32GetDatum(query_hash);
	values[1] = CStringGetTextDatum(query_text);

	query_index_rel_oid = RelnameGetRelid("aqo_query_texts_query_hash_idx");
	if (!OidIsValid(query_index_rel_oid))
	{
		disable_aqo_for_query();
		return false;
	}
	query_index_rel = index_open(query_index_rel_oid, heap_lock);

	aqo_query_texts_table_rv = makeRangeVar("public",
											"aqo_query_texts",
											-1);
	aqo_query_texts_heap = heap_openrv(aqo_query_texts_table_rv,
									   heap_lock);

	tuple = heap_form_tuple(RelationGetDescr(aqo_query_texts_heap),
							values, nulls);

	PG_TRY();
	{
		simple_heap_insert(aqo_query_texts_heap, tuple);
		index_insert(query_index_rel,
					 values, nulls,
					 &(tuple->t_self),
					 aqo_query_texts_heap,
					 UNIQUE_CHECK_YES);
	}
	PG_CATCH();
	{
		CommandCounterIncrement();
		simple_heap_delete(aqo_query_texts_heap, &(tuple->t_self));
	}
	PG_END_TRY();


	index_close(query_index_rel, heap_lock);
	heap_close(aqo_query_texts_heap, heap_lock);

	CommandCounterIncrement();

	return true;
}

/*
 * Loads feature subspace (fss) from table aqo_data into memory.
 * The last column of the returned matrix is for target values of objects.
 * Returns false if the operation failed, true otherwise.
 *
 * 'fss_hash' is the hash of feature subspace which is supposed to be loaded
 * 'ncols' is the number of clauses in the feature subspace
 * 'matrix' is an allocated memory for matrix with the size of aqo_K rows
 *			and nhashes colums
 * 'targets' is an allocated memory with size aqo_K for target values
 *			of the objects
 * 'rows' is the pointer in which the function stores actual number of
 *			objects in the given feature space
 */
bool
load_fss(int fss_hash, int ncols,
		 double **matrix, double *targets, int *rows)
{
	RangeVar   *aqo_data_table_rv;
	Relation	aqo_data_heap;
	HeapTuple	tuple;
	LOCKMODE	heap_lock = AccessShareLock;

	Relation	data_index_rel;
	Oid			data_index_rel_oid;
	IndexScanDesc data_index_scan;
	ScanKeyData *key;

	Datum		values[5];
	bool		nulls[5];

	bool		success = true;

	data_index_rel_oid = RelnameGetRelid("aqo_fss_access_idx");
	if (!OidIsValid(data_index_rel_oid))
	{
		disable_aqo_for_query();
		return false;
	}

	aqo_data_table_rv = makeRangeVar("public", "aqo_data", -1);
	aqo_data_heap = heap_openrv(aqo_data_table_rv, heap_lock);

	data_index_rel = index_open(data_index_rel_oid, heap_lock);
	data_index_scan = index_beginscan(
									  aqo_data_heap,
									  data_index_rel,
									  SnapshotSelf,
									  2,
									  0
		);

	key = palloc(sizeof(*key) * 2);
	ScanKeyInit(&key[0],
				1,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(fspace_hash)
		);
	ScanKeyInit(&key[1],
				2,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(fss_hash)
		);

	index_rescan(data_index_scan, key, 2, NULL, 0);

	tuple = index_getnext(data_index_scan, ForwardScanDirection);

	if (tuple != NULL)
	{
		heap_deform_tuple(tuple, aqo_data_heap->rd_att, values, nulls);

		if (DatumGetInt32(values[2]) == ncols)
		{
			deform_matrix(values[3], matrix);
			deform_vector(values[4], targets, rows);
		}
		else
		{
			elog(WARNING, "unexpected number of features for hash (%d, %d):\
						   expected %d features, obtained %d", fspace_hash,
				 fss_hash, ncols, DatumGetInt32(values[2]));
			success = false;
		}
	}
	else
		success = false;

	index_endscan(data_index_scan);

	index_close(data_index_rel, heap_lock);
	heap_close(aqo_data_heap, heap_lock);

	pfree(key);

	return success;
}

/*
 * Updates the specified line in the specified feature subspace.
 * Returns false if the operation failed, true otherwise.
 *
 * 'fss_hash' specifies the feature subspace
 * 'nrows' x 'ncols' is the shape of 'matrix'
 * 'targets' is vector of size 'nrows'
 * 'old_nrows' is previous number of rows in matrix
 * 'changed_rows' is an integer list of changed lines
 */
bool
update_fss(int fss_hash, int nrows, int ncols, double **matrix, double *targets,
		   int old_nrows, List *changed_rows)
{
	RangeVar   *aqo_data_table_rv;
	Relation	aqo_data_heap;
	HeapTuple	tuple,
				nw_tuple;
	LOCKMODE	heap_lock = RowExclusiveLock;

	Relation	data_index_rel;
	Oid			data_index_rel_oid;
	ScanKeyData *key;
	IndexScanDesc data_index_scan;
	LOCKMODE	index_lock = RowExclusiveLock;

	Datum		values[5];
	bool		nulls[5] = {false, false, false, false, false};
	bool		do_replace[5] = {false, false, false, true, true};

	TupleDesc	tuple_desc;

	data_index_rel_oid = RelnameGetRelid("aqo_fss_access_idx");
	if (!OidIsValid(data_index_rel_oid))
	{
		disable_aqo_for_query();
		return false;
	}

	aqo_data_table_rv = makeRangeVar("public", "aqo_data", -1);
	aqo_data_heap = heap_openrv(aqo_data_table_rv, heap_lock);

	tuple_desc = RelationGetDescr(aqo_data_heap);

	data_index_rel = index_open(data_index_rel_oid, index_lock);
	data_index_scan = index_beginscan(
									  aqo_data_heap,
									  data_index_rel,
									  SnapshotSelf,
									  2,
									  0
		);

	key = palloc(sizeof(*key) * 2);
	ScanKeyInit(&key[0],
				1,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(fspace_hash)
		);
	ScanKeyInit(&key[1],
				2,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(fss_hash)
		);

	index_rescan(data_index_scan, key, 2, NULL, 0);

	tuple = index_getnext(data_index_scan, ForwardScanDirection);

	if (tuple == NULL)
	{
		values[0] = Int32GetDatum(fspace_hash);
		values[1] = Int32GetDatum(fss_hash);
		values[2] = Int32GetDatum(ncols);
		values[3] = PointerGetDatum(form_matrix(matrix, nrows, ncols));
		values[4] = PointerGetDatum(form_vector(targets, nrows));
		tuple = heap_form_tuple(tuple_desc, values, nulls);
		PG_TRY();
		{
			simple_heap_insert(aqo_data_heap, tuple);
			index_insert(data_index_rel, values, nulls, &(tuple->t_self),
						 aqo_data_heap, UNIQUE_CHECK_YES);
		}
		PG_CATCH();
		{
			CommandCounterIncrement();
			simple_heap_delete(aqo_data_heap, &(tuple->t_self));
		}
		PG_END_TRY();
	}
	else
	{
		heap_deform_tuple(tuple, aqo_data_heap->rd_att, values, nulls);
		values[3] = PointerGetDatum(form_matrix(matrix, nrows, ncols));
		values[4] = PointerGetDatum(form_vector(targets, nrows));
		nw_tuple = heap_modify_tuple(tuple, tuple_desc,
									 values, nulls, do_replace);
		if (my_simple_heap_update(aqo_data_heap, &(nw_tuple->t_self), nw_tuple))
		{
			index_insert(data_index_rel, values, nulls, &(nw_tuple->t_self),
						 aqo_data_heap, UNIQUE_CHECK_YES);
		}
		else
		{
			/*
			 * Ooops, somebody concurrently updated the tuple. We have to merge
			 * our changes somehow, but now we just discard ours. We don't
			 * believe in high probability of simultaneously finishing of two
			 * long, complex, and important queries, so we don't loss important
			 * data.
			 */
		}
	}

	index_endscan(data_index_scan);

	index_close(data_index_rel, index_lock);
	heap_close(aqo_data_heap, heap_lock);

	pfree(key);

	CommandCounterIncrement();

	return true;
}

QueryStat *
get_aqo_stat(int query_hash)
{
	RangeVar   *aqo_stat_table_rv;
	Relation	aqo_stat_heap;
	HeapTuple	tuple;
	LOCKMODE	heap_lock = AccessShareLock;

	Relation	stat_index_rel;
	Oid			stat_index_rel_oid;
	IndexScanDesc stat_index_scan;
	ScanKeyData key;

	Datum		values[9];
	bool		nulls[9];

	QueryStat  *stat = palloc_query_stat();

	stat_index_rel_oid = RelnameGetRelid("aqo_query_stat_idx");
	if (!OidIsValid(stat_index_rel_oid))
	{
		disable_aqo_for_query();
		return NULL;
	}

	aqo_stat_table_rv = makeRangeVar("public", "aqo_query_stat", -1);
	aqo_stat_heap = heap_openrv(aqo_stat_table_rv, heap_lock);

	stat_index_rel = index_open(stat_index_rel_oid, heap_lock);
	stat_index_scan = index_beginscan(
									  aqo_stat_heap,
									  stat_index_rel,
									  SnapshotSelf,
									  1,
									  0
		);

	ScanKeyInit(&key,
				1,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(query_hash)
		);

	index_rescan(stat_index_scan, &key, 1, NULL, 0);

	tuple = index_getnext(stat_index_scan, ForwardScanDirection);

	if (tuple != NULL)
	{
		heap_deform_tuple(tuple, aqo_stat_heap->rd_att, values, nulls);

		deform_vector(values[1], stat->execution_time_with_aqo, &stat->execution_time_with_aqo_size);
		deform_vector(values[2], stat->execution_time_without_aqo, &stat->execution_time_without_aqo_size);
		deform_vector(values[3], stat->planning_time_with_aqo, &stat->planning_time_with_aqo_size);
		deform_vector(values[4], stat->planning_time_without_aqo, &stat->planning_time_without_aqo_size);
		deform_vector(values[5], stat->cardinality_error_with_aqo, &stat->cardinality_error_with_aqo_size);
		deform_vector(values[6], stat->cardinality_error_without_aqo, &stat->cardinality_error_without_aqo_size);
		stat->executions_with_aqo = DatumGetInt64(values[7]);
		stat->executions_without_aqo = DatumGetInt64(values[8]);
	}

	index_endscan(stat_index_scan);

	index_close(stat_index_rel, heap_lock);
	heap_close(aqo_stat_heap, heap_lock);

	return stat;
}

void
update_aqo_stat(int query_hash, QueryStat * stat)
{
	RangeVar   *aqo_stat_table_rv;
	Relation	aqo_stat_heap;
	HeapTuple	tuple,
				nw_tuple;
	LOCKMODE	heap_lock = RowExclusiveLock;

	Relation	stat_index_rel;
	Oid			stat_index_rel_oid;
	IndexScanDesc stat_index_scan;
	ScanKeyData key;
	LOCKMODE	index_lock = RowExclusiveLock;

	Datum		values[9];
	bool		nulls[9] = {false, false, false,
							false, false, false,
							false, false, false};
	bool		do_replace[9] = {false, true, true,
							true, true, true,
							true, true, true};

	TupleDesc	tuple_desc;


	stat_index_rel_oid = RelnameGetRelid("aqo_query_stat_idx");
	if (!OidIsValid(stat_index_rel_oid))
	{
		disable_aqo_for_query();
		return;
	}

	aqo_stat_table_rv = makeRangeVar("public", "aqo_query_stat", -1);
	aqo_stat_heap = heap_openrv(aqo_stat_table_rv, heap_lock);

	tuple_desc = RelationGetDescr(aqo_stat_heap);

	stat_index_rel = index_open(stat_index_rel_oid, index_lock);
	stat_index_scan = index_beginscan(
									  aqo_stat_heap,
									  stat_index_rel,
									  SnapshotSelf,
									  1,
									  0
		);

	ScanKeyInit(&key,
				1,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(query_hash)
		);

	index_rescan(stat_index_scan, &key, 1, NULL, 0);

	tuple = index_getnext(stat_index_scan, ForwardScanDirection);

	if (tuple == NULL)
		values[0] = Int32GetDatum(query_hash);
	else
		heap_deform_tuple(tuple, aqo_stat_heap->rd_att, values, nulls);

	values[1] = PointerGetDatum(form_vector(stat->execution_time_with_aqo, stat->execution_time_with_aqo_size));
	values[2] = PointerGetDatum(form_vector(stat->execution_time_without_aqo, stat->execution_time_without_aqo_size));
	values[3] = PointerGetDatum(form_vector(stat->planning_time_with_aqo, stat->planning_time_with_aqo_size));
	values[4] = PointerGetDatum(form_vector(stat->planning_time_without_aqo, stat->planning_time_without_aqo_size));
	values[5] = PointerGetDatum(form_vector(stat->cardinality_error_with_aqo, stat->cardinality_error_with_aqo_size));
	values[6] = PointerGetDatum(form_vector(stat->cardinality_error_without_aqo, stat->cardinality_error_without_aqo_size));
	values[7] = Int64GetDatum(stat->executions_with_aqo);
	values[8] = Int64GetDatum(stat->executions_without_aqo);

	if (tuple == NULL)
	{
		tuple = heap_form_tuple(tuple_desc, values, nulls);
		PG_TRY();
		{
			simple_heap_insert(aqo_stat_heap, tuple);
			index_insert(stat_index_rel, values, nulls, &(tuple->t_self),
						 aqo_stat_heap, UNIQUE_CHECK_YES);
		}
		PG_CATCH();
		{
			CommandCounterIncrement();
			simple_heap_delete(aqo_stat_heap, &(tuple->t_self));
		}
		PG_END_TRY();
	}
	else
	{
		nw_tuple = heap_modify_tuple(tuple, tuple_desc,
									 values, nulls, do_replace);
		if (my_simple_heap_update(aqo_stat_heap, &(nw_tuple->t_self), nw_tuple))
		{
			index_insert(stat_index_rel, values, nulls, &(nw_tuple->t_self),
						 aqo_stat_heap, UNIQUE_CHECK_YES);
		}
		else
		{
			/*
			 * Ooops, somebody concurrently updated the tuple. We have to merge
			 * our changes somehow, but now we just discard ours. We don't
			 * believe in high probability of simultaneously finishing of two
			 * long, complex, and important queries, so we don't loss important
			 * data.
			 */
		}
	}

	index_endscan(stat_index_scan);

	index_close(stat_index_rel, heap_lock);
	heap_close(aqo_stat_heap, heap_lock);

	CommandCounterIncrement();
}

/*
 * Expands matrix from storage into simple C-array.
 */
void
deform_matrix(Datum datum, double **matrix)
{
	ArrayType  *array = DatumGetArrayTypePCopy(datum);
	int			nelems;
	Datum	   *values;
	int			rows;
	int			cols;
	int			i,
				j;

	deconstruct_array(array,
					  FLOAT8OID, 8, true, 'd',
					  &values, NULL, &nelems);
	rows = ARR_DIMS(array)[0];
	cols = ARR_DIMS(array)[1];
	for (i = 0; i < rows; ++i)
		for (j = 0; j < cols; ++j)
			matrix[i][j] = DatumGetFloat8(values[i * cols + j]);
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
	ArrayType  *array = DatumGetArrayTypePCopy(datum);
	Datum	   *values;
	int			i;

	deconstruct_array(array,
					  FLOAT8OID, 8, true, 'd',
					  &values, NULL, nelems);
	for (i = 0; i < *nelems; ++i)
		vector[i] = DatumGetFloat8(values[i]);
	pfree(values);
	pfree(array);
}

/*
 * Froms ArrayType object for storage from simple C-array matrix.
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
							   FLOAT8OID, 8, true, 'd');
	pfree(elems);
	return array;
}

/*
 * Froms ArrayType object for storage from simple C-array vector.
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
							   FLOAT8OID, 8, true, 'd');
	pfree(elems);
	return array;
}

/*
 * Return true if updated successfully, false if updated concurrently by
 * another session, error otherwise.
 */
static bool
my_simple_heap_update(Relation relation, ItemPointer otid, HeapTuple tup)
{
	HTSU_Result result;
	HeapUpdateFailureData hufd;
	LockTupleMode lockmode;

	result = heap_update(relation, otid, tup,
						 GetCurrentCommandId(true), InvalidSnapshot,
						 true /* wait for commit */ ,
						 &hufd, &lockmode);
	switch (result)
	{
		case HeapTupleSelfUpdated:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case HeapTupleMayBeUpdated:
			/* done successfully */
			return true;
			break;

		case HeapTupleUpdated:
			return false;
			break;

		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			break;
	}
}

/* Creates a storage for hashes of deactivated queries */
void
init_deactivated_queries_storage(void)
{
	HASHCTL	hash_ctl;

	/* Create the hashtable proper */
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(int);
	hash_ctl.entrysize = sizeof(int);
	deactivated_queries = hash_create("aqo_deactivated_queries",
									  128,  /* start small and extend */
									  &hash_ctl,
									  HASH_ELEM);
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
	bool	found;

	hash_search(deactivated_queries, &query_hash, HASH_FIND, &found);
	return found;
}

/* Adds given query hash into the set of hashes of deactivated queries*/
void
add_deactivated_query(int query_hash)
{
	bool	found;

	hash_search(deactivated_queries, &query_hash, HASH_ENTER, &found);
}
