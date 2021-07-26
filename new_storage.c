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
#include "storage/fd.h"
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

void makeAqoDir(void);
void createAqoFile(const char *name);
void file_add_query_text(int qhash, const char *query_string);
void file_read_query_text();

/* 
 * createAqoFile
 */
void
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
	query_note *qnote = calloc(query_note_len, 1);

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

void
file_read_query_text()
{
	char path[MAXPGPATH];
	int fd;
	int qhash;
	char *query_string;
	unsigned int query_note_len;
	query_note *qnote;

	AqoFilePath(path, "aqo_query_texts");
	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	while (read(fd, &query_note_len, sizeof(query_note_len)))
	{
		qnote = calloc(query_note_len, 1);
		if (read(fd, qnote, query_note_len) < 0) 
		{
			elog(ERROR, "could not read file \"%s\": %m", path);
		}
		qhash = qnote->qhash;
		query_string = calloc(query_note_len, 1);
		strcpy(query_string, qnote->query_string);
		printf("%d ", qhash);
		printf("%s", query_string);
	}
	close(fd);
}