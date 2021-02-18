#include "aqo.h"
#include "ignorance.h"

#include "access/heapam.h"
#include "access/parallel.h"
#include "executor/spi.h"
#include "utils/lsyscache.h"
#include "miscadmin.h"

bool aqo_log_ignorance;

void
set_ignorance(bool newval, void *extra)
{
	/*
	 * On postgres start we can't create any table.
	 * It is not problem. We will check existence at each update and create this
	 * table in dynamic mode, if needed.
	 */
	if (IsUnderPostmaster && !IsParallelWorker() && newval &&
		(aqo_log_ignorance != newval))
		/* Create storage and no error, if it exists already. */
		create_ignorance_table(true);

	aqo_log_ignorance = newval;
}

bool
create_ignorance_table(bool fail_ok)
{
	Oid nspid = get_aqo_schema();
	char *nspname;
	char *sql;
	int rc;

	if (nspid == InvalidOid)
	{
		if (!fail_ok)
			ereport(ERROR,
					(errmsg("AQO extension is not installed"),
					errdetail("AQO shared library is enabled but extension isn't installed.")));
		else
			return false;
	}

	nspname = get_namespace_name(nspid);
	Assert(nspname != NULL);

	/* Check the table existence. */
	if (get_relname_relid("aqo_ignorance", nspid) != InvalidOid)
	{
		if (!fail_ok)
			elog(PANIC, "aqo_ignorance table exists yet.");
		else
			return false;
	}

	sql = psprintf("CREATE TABLE %s.aqo_ignorance (qhash int, fhash int, fss_hash int, node_type int, node text);"
				   "CREATE UNIQUE INDEX aqo_ignorance_idx ON aqo_ignorance (qhash, fhash, fss_hash);",
					nspname);

	SPI_connect();
	rc = SPI_execute(sql, false, 0);
	SPI_finish();

	if (rc < 0)
		/* Can't ignore this problem. */
		elog(ERROR, "Failed to create aqo_ignorance table %s. status: %d",
			 sql, rc);

	pfree(nspname);
	pfree(sql);
	return true;
}

void
update_ignorance(int qhash, int fhash, int fss_hash, Plan *plan)
{
	RangeVar	*rv;
	Relation	hrel;
	Relation	irel;
	SnapshotData snap;
	TupleTableSlot *slot;
	TupleDesc	tupDesc;
	HeapTuple	tuple;
	Datum		values[5];
	bool		isnull[5] = { false, false, false, false, false };
	bool		shouldFree;
	Oid			reloid;
	IndexScanDesc scan;
	ScanKeyData	key[3];
	LOCKTAG		tag;
	Oid			nspid = get_aqo_schema();
	char		*nspname;

	if (!OidIsValid(nspid))
		elog(PANIC, "AQO schema does not exists!");
	nspname = get_namespace_name(nspid);
	Assert(nspname != 0);

	rv = makeRangeVar(nspname, "aqo_ignorance_idx", -1);
	reloid = RangeVarGetRelid(rv, NoLock, true);
	if (!OidIsValid(reloid))
	{
		/* This table doesn't created on instance startup. Create now. */
		create_ignorance_table(false);
		reloid = RangeVarGetRelid(rv, NoLock, true);
			if (!OidIsValid(reloid))
				elog(PANIC, "Ignorance table does not exists!");
	}

	init_lock_tag(&tag, (uint32) fhash, (uint32) fss_hash);
	LockAcquire(&tag, ExclusiveLock, false, false);

	rv = makeRangeVar(nspname, "aqo_ignorance", -1);
	hrel = table_openrv(rv, RowExclusiveLock);
	irel = index_open(reloid, RowExclusiveLock);
	tupDesc = RelationGetDescr(hrel);

	InitDirtySnapshot(snap);
	scan = index_beginscan(hrel, irel, &snap, 3, 0);

	ScanKeyInit(&key[0], 1, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(qhash));
	ScanKeyInit(&key[1], 2, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(fhash));
	ScanKeyInit(&key[2], 3, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(fss_hash));
	index_rescan(scan, key, 3, NULL, 0);
	slot = MakeSingleTupleTableSlot(tupDesc, &TTSOpsBufferHeapTuple);

	if (!index_getnext_slot(scan, ForwardScanDirection, slot))
	{
		if (plan->predicted_cardinality < 0.)
		{
			char nodestr[1024];
			char *qplan = nodeToString(plan);

			memset(nodestr, 0, 1024);
			strncpy(nodestr, qplan, 1023);
			pfree(qplan);

			/*
			 * AQO failed to predict cardinality for this node.
			 */
			values[0] = Int32GetDatum(qhash);
			values[1] = Int32GetDatum(fhash);
			values[2] = Int32GetDatum(fss_hash);
			values[3] = Int32GetDatum(nodeTag(plan));
			values[4] = CStringGetTextDatum(nodestr);
			tuple = heap_form_tuple(tupDesc, values, isnull);

			simple_heap_insert(hrel, tuple);
			my_index_insert(irel, values, isnull, &(tuple->t_self),
														hrel, UNIQUE_CHECK_YES);
		}
		else
		{
			/* AQO works as expected. */
		}
	}
	else if (!TransactionIdIsValid(snap.xmin) &&
			 !TransactionIdIsValid(snap.xmax))
	{
		/*
		 * AQO made prediction for this node. Delete it from the ignorance
		 * table.
		 */
		tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
		Assert(shouldFree != true);
		simple_heap_delete(hrel, &(tuple->t_self));
	}
	else
	{
		/*
		 * The data exists. We can't do anything for now.
		 */
	}

	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	index_close(irel, RowExclusiveLock);
	table_close(hrel, RowExclusiveLock);

	CommandCounterIncrement();
	LockRelease(&tag, ExclusiveLock, false);
}
