/*
 *******************************************************************************
 *
 * NEW STORAGE
 *
 * This module is responsible for organization and interaction with the storage
 * catalog pg_aqo in PGDATA
 *
 * IDENTIFICATION
 *	  aqo/new_storage.c
*/

#include "aqo.h"
#include "new_storage.h"
#include "storage/fd.h"
#include "funcapi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>

typedef struct query_note
{
	int qhash;
	char query_string[INT_MAX];
} query_note;

#define AQO_DIR "pg_aqo"

#define AqoFilePath(path, table_name) \
	snprintf(path, MAXPGPATH, AQO_DIR "/%s.bin", table_name)

bool use_file_storage;

static void createAqoFile(const char *name);

/*
 * createAqoFile
 */
static void
createAqoFile(const char *name)
{
	char path[MAXPGPATH];
	int fd;
	int qhash = 0;

	AqoFilePath(path, name);
	fd = BasicOpenFile(path, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY);
	if (fd < 0)
	{
		elog(ERROR, "could not create file \"%s\": %m", path);
	}
	file_add_query_text(qhash, "COMMON feature space (do not delete!)");
	close(fd);
}

/*
 * makeAqoDir
 */
void
makeAqoDir(void)
{
	if (MakePGDirectory(AQO_DIR) < 0)
	{
		elog(ERROR, "could not create directory \"%s\": %m", AQO_DIR);
	}

	createAqoFile("aqo_query_texts");
}

/*
 * file_add_query_text
 * add note which consists of service information (length of a query_string),
 * qhash and the query_string in .bin file $PGDATA/pg_aqo/aqo_query_texts.bin
 */
void
file_add_query_text(int qhash, const char *query_string)
{
	char path[MAXPGPATH];
	int fd;
	unsigned int query_note_len = sizeof(qhash) + strlen(query_string) + 1;
	query_note *qnote = palloc(query_note_len);

	qnote->qhash = qhash;
	strcpy(qnote->query_string, query_string);
	AqoFilePath(path, "aqo_query_texts");
	fd = BasicOpenFile(path, O_WRONLY | O_APPEND | O_CREAT);
	if (fd < 0)
	{
		elog(ERROR, "could not open file \"%s\": %m", path);
	}
	if (write(fd, &query_note_len, sizeof(query_note_len)) < 0)
	{
		elog(ERROR, "could not write in file \"%s\": %m", path);
	}
	if (write(fd, qnote, query_note_len) < 0)
	{
		elog(ERROR, "could not write qnote in file \"%s\": %m", path);
	}
	close(fd);
}

PG_FUNCTION_INFO_V1(file_read_query_text);
Datum
file_read_query_text(PG_FUNCTION_ARGS)
{
#define AQO_QUERY_TEXTS_COLS	2
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		 tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	 per_query_ctx;
	MemoryContext	 oldcontext;
	int				 i;

	char path[MAXPGPATH];
	int fd;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* The tupdesc and tuplestore must be created in ecxt_per_query_memory */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* build tupdesc for result tuples. */
	tupdesc = CreateTemplateTupleDesc(AQO_QUERY_TEXTS_COLS);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "query_hash", 
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "query_text", 
					   TEXTOID, -1, 0);

	tupstore = tuplestore_begin_heap(rsinfo->allowedModes & SFRM_Materialize_Random,
									false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	AqoFilePath(path, "aqo_query_texts");
	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	for(i = 0; ; ++i)
	{
		Datum	values[AQO_QUERY_TEXTS_COLS];
		bool	nulls[AQO_QUERY_TEXTS_COLS];
		
		text		*query_string;
		unsigned int query_note_len;
		query_note  *qnote;
		int32		 qhash;

		if(read(fd, &query_note_len, sizeof(query_note_len)) <= 0)
			break;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		qnote = palloc(query_note_len);
		if (read(fd, qnote, query_note_len) < 0)
		{
			elog(ERROR, "could not read file \"%s\": %m", path);
		}
		qhash = (int32) qnote->qhash;
		query_string = cstring_to_text_with_len(qnote->query_string, query_note_len);

		values[0] = Int32GetDatum(qhash);
		values[1] = PointerGetDatum(query_string);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
	close(fd);

	return (Datum) 0;
}
