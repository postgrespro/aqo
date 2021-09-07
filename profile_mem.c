#include "aqo.h"
#include "profile_mem.h"

#include "funcapi.h"
#include "miscadmin.h"

int 	aqo_profile_mem;
bool	out_of_memory = false;
int i = 0;
static HTAB   *profile_mem_queries = NULL;

typedef struct ProfileMemEntry
{
	int key;
	double time;
} ProfileMemEntry;

PG_FUNCTION_INFO_V1(aqo_profile_mem_hash);

Datum
aqo_profile_mem_hash(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hash_seq;
	ProfileMemEntry *entry;
    TupleDesc tupdesc;
	HeapTuple tuple;
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

	if (!profile_mem_queries)
	{
		ReleaseTupleDesc(tupdesc);
		tuplestore_donestoring(tupstore);
		elog(WARNING, "Hash table 'profile_mem_queries' doesn't exist");
		PG_RETURN_VOID();
	}

	hash_seq_init(&hash_seq, profile_mem_queries);
	while (((entry = (ProfileMemEntry *) hash_seq_search(&hash_seq)) != NULL))
	{
		char **values;
		
		values = (char **) palloc(2 * sizeof(char *));
		values[0] = (char *) palloc(16 * sizeof(char));
		values[1] = (char *) palloc(16 * sizeof(char));
		
		snprintf(values[0], 16, "%d", entry->key);
		snprintf(values[1], 16, "%0.5f", entry->time);
		
		tuple = BuildTupleFromCStrings(attinmeta, values);
		tuplestore_puttuple(tupstore, tuple);
	}

	ReleaseTupleDesc(tupdesc);
	tuplestore_donestoring(tupstore);

	PG_RETURN_VOID();
}

void
set_profile_mem(int newval, void *extra)
{
	aqo_profile_mem = newval;
	if (aqo_profile_mem > 0)
	{
		HASHCTL hash_ctl;
		hash_ctl.keysize = sizeof(int);
		hash_ctl.entrysize = sizeof(ProfileMemEntry);
		profile_mem_queries = ShmemInitHash("aqo_profile_mem_queries", aqo_profile_mem, aqo_profile_mem, &hash_ctl, HASH_ELEM | HASH_BLOBS);
	}
}

void
update_profile_mem_table(void)
{
	bool found;
    ProfileMemEntry *pentry;
    double totaltime;
    instr_time endtime;

    if (aqo_profile_mem > 0)
	{
		INSTR_TIME_SET_CURRENT(endtime);
		INSTR_TIME_SUBTRACT(endtime, query_context.query_starttime);
		totaltime = INSTR_TIME_GET_DOUBLE(endtime);

        PG_TRY();
		{
			if (!out_of_memory)
			{
				pentry = (ProfileMemEntry *) hash_search(profile_mem_queries, &query_context.query_hash, HASH_ENTER, &found);
				if (found)
					pentry->time += totaltime - query_context.query_planning_time;
				else
					pentry->time = totaltime - query_context.query_planning_time;
			}
		}
 		PG_CATCH();
		{
			elog(LOG, "Failed to change aqo_profile_mem_queries table.");
			out_of_memory = true;
		}
 		PG_END_TRY();
	}
}