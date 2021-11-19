/*
 *******************************************************************************
 *
 * FILE STORAGE
 *
 * This module is responsible for organization and interaction with the storage
 * catalog pg_aqo in PGDATA
 *
 * IDENTIFICATION
 *	  aqo/file_storage.c
*/

#include "aqo.h"
#include "file_storage.h"
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


#define AQO_DIR "pg_aqo"

#define NUMENTRIES 10000	/* number of entries in hash table */

#define AqoFilePath(path, table_name) \
	snprintf(path, MAXPGPATH, AQO_DIR "/%s.bin", table_name)

typedef struct aqoqtEntry
{
	int qhash;
	Size			query_offset;			/* query text offset in external file */
	unsigned int	query_len;				/* number of valid bytes in query string */
} aqoqtEntry;

bool	use_file_storage;
static	Size extent = 0;
static	HTAB *aqoqt_hash = NULL;

static void createAqoFile(const char *name);
void makeAqoDir(void);
void file_add_query_text(int qhash, const char *query_string);
static bool qtext_store(int qhash, const char *query, int query_len, Size *query_offset);
static aqoqtEntry *entry_alloc(int *qhash, Size query_offset, int query_len);
void createAqoQT(void);
void aqoqt_hash_recovery(void);

/*
 * createAqoFile
 */
static void
createAqoFile(const char *name)
{
	char	path[MAXPGPATH];
	int		fd;

	AqoFilePath(path, name);
	fd = BasicOpenFile(path, O_CREAT | O_WRONLY | PG_BINARY);
	if (fd < 0)
	{
		elog(ERROR, "could not create file \"%s\": %m", path);
	}

	close(fd);
}

void
createAqoQT(void)
{
	HASHCTL		info;
	aqoqtEntry	tmp;

	createAqoFile("aqo_query_texts");
	info.keysize = sizeof(tmp.qhash);
	info.entrysize = sizeof(aqoqtEntry);
	aqoqt_hash = hash_create("Aqo_query_texts hash",
							NUMENTRIES, &info, HASH_ELEM | HASH_BLOBS);

	/* file_add_query_text(0, "COMMON feature space (do not delete!)"); */
}

void
aqoqt_hash_recovery(void)
{
	int			fd;
	char		path[MAXPGPATH];
	int			qhash;
	int			qlen;
	char		*qstring;
	Size		query_offset;
	Size		off;

	HASHCTL		info;
	aqoqtEntry	tmp;

	info.keysize = sizeof(tmp.qhash);
	info.entrysize = sizeof(aqoqtEntry);
	aqoqt_hash = hash_create("Aqo_query_texts hash",
							NUMENTRIES, &info, HASH_ELEM | HASH_BLOBS);

	extent = 0;
	AqoFilePath(path, "aqo_query_texts");
	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0) {
		elog(ERROR, "could not open file \"%s\": %m", path);
	}

	while(read(fd, &qhash, sizeof(qhash)) == sizeof(qhash))
	{
		if (read(fd, &qlen, sizeof(qlen)) != sizeof(qlen))
		{
			ereport(LOG,
					(errcode_for_file_access(),
					errmsg("could not read file \"%s\": %m",
					path)));
		}

		qstring = (char *) palloc(qlen + 1);
		if (read(fd, qstring, qlen + 1) != qlen + 1)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					errmsg("could not read file \"%s\": %m",
					path)));
		}

		off = extent;
		extent += sizeof(qhash) + sizeof(qlen) + qlen + 1;
		query_offset = off + sizeof(qhash) + sizeof(qlen);
		entry_alloc(&qhash, query_offset, qlen);
	}
}

/*
 * makeAqoDir
 */
void
makeAqoDir(void)
{
	if (MakePGDirectory(AQO_DIR) < 0)
	{
		if (MakePGDirectory(AQO_DIR) < 0  && errno != EEXIST)
		{
			elog(ERROR, "could not create directory \"%s\": %m", AQO_DIR);
		}
		aqoqt_hash_recovery();
	}
	else
	{
		createAqoQT();
	}
}

/*
 * file_add_query_text
 * add entry in .bin file $PGDATA/pg_aqo/aqo_query_texts.bin and in hash table
 */
void
file_add_query_text(int qhash, const char *query_string)
{
	int				query_len = strlen(query_string);
	aqoqtEntry		*entry;

	entry = (aqoqtEntry *) hash_search(aqoqt_hash, &qhash, HASH_FIND, NULL);
	if(!entry)
	{
		Size		query_offset;
		//bool		stored;

		// stored = qtext_store(query_string, query_len, &query_offset);
		qtext_store(qhash, query_string, query_len, &query_offset);

		entry = entry_alloc(&qhash, query_offset, query_len);
	}
}

static bool
qtext_store(int qhash,
			const char *query, 
			int query_len,
			Size *query_offset)
{
	Size		off;
	int			fd;
	char		path[MAXPGPATH];

	off = extent;
	extent += sizeof(qhash) + sizeof(query_len) + query_len + 1;

	*query_offset = off + sizeof(qhash) + sizeof(query_len);

	/* Now write the data into the successfully-reserved part of the file */
	AqoFilePath(path, "aqo_query_texts");
	fd = BasicOpenFile(path, O_WRONLY | O_APPEND | O_CREAT);
	if (fd < 0)
		goto error;

	if (pg_pwrite(fd, &qhash, sizeof(qhash), off) != sizeof(qhash))
		goto error;
	if (pg_pwrite(fd, &query_len, sizeof(query_len), off + sizeof(qhash)) != sizeof(query_len))
		goto error;
	if (pg_pwrite(fd, query, query_len, off + sizeof(qhash) + sizeof(query_len)) != query_len)
		goto error;
	if (pg_pwrite(fd, "\0", 1, off + sizeof(qhash) + sizeof(query_len) + query_len) != 1)
		goto error;

	close(fd);

	return true;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write file \"%s\": %m", path)));

	if (fd >= 0)
		close(fd);

	return false;
}

PG_FUNCTION_INFO_V1(file_read_query_text);
Datum
file_read_query_text(PG_FUNCTION_ARGS)
{
#define AQO_QUERY_TEXTS_COLS	2
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;

	char			path[MAXPGPATH];
	int				fd;
	int				buffer_size;
	char			*buffer = NULL;
	aqoqtEntry		*entry;
	HASH_SEQ_STATUS	hash_seq;

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
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	AqoFilePath(path, "aqo_query_texts");
	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0) {
		elog(ERROR, "could not open file \"%s\": %m", path);
	}

	buffer_size = 2048;
	buffer = (char *) palloc(buffer_size);

	hash_seq_init(&hash_seq, aqoqt_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum	values[AQO_QUERY_TEXTS_COLS];
		bool	nulls[AQO_QUERY_TEXTS_COLS];
		
		int			i = 0;
		int			qlen = entry->query_len;
		int			qhash;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		if (qlen >= buffer_size)
		{
			buffer_size = Max(buffer_size * 2, qlen + 1);
			buffer = repalloc(buffer, buffer_size);
		}
		if (read(fd, &qhash, sizeof(qhash)) != sizeof(qhash))
			ereport(LOG,
					(errcode_for_file_access(),
					errmsg("could not read file \"%s\": %m",
					path)));
		if (read(fd, &qlen, sizeof(qlen)) != sizeof(qlen))
			ereport(LOG,
					(errcode_for_file_access(),
					errmsg("could not read file \"%s\": %m",
					path)));
		if (read(fd, buffer, qlen + 1) != qlen + 1)
			ereport(LOG,
					(errcode_for_file_access(),
					errmsg("could not read file \"%s\": %m",
					path)));
		/* Should have a trailing null, but let's make sure */
		buffer[qlen] = '\0';

		values[i++] = Int32GetDatum(entry->qhash);
		values[i++] = CStringGetTextDatum(buffer);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
	close(fd);

	return (Datum) 0;
}

static aqoqtEntry *
entry_alloc(int *qhash, Size query_offset, int query_len)
{
	aqoqtEntry  *entry;
	bool		found;

	/* Find or create an entry with desired hash code */
	entry = (aqoqtEntry *) hash_search(aqoqt_hash, qhash, HASH_ENTER, &found);

	if (!found)
	{
		/* The query text metadata */
		Assert(query_len >= 0);
		entry->query_offset = query_offset;
		entry->query_len = query_len;
	}

	return entry;
}
