/*
 *******************************************************************************
 *
 *	SELECTIVITY CACHE
 *
 * Stores the clause selectivity with the given relids for parametrized
 * clauses, because otherwise it cannot be restored after query execution
 * without PlannerInfo.
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/selectivity_cache.c
 *
 */

#include "postgres.h"

#include "aqo.h"

typedef struct
{
	int			clause_hash;
	int			relid;
	int			global_relid;
	double		selectivity;
}	Entry;

List	   *objects = NIL;

/*
 * Stores the given selectivity for clause_hash, relid and global_relid
 * of the clause.
 */
void
cache_selectivity(int clause_hash,
				  int relid,
				  int global_relid,
				  double selectivity)
{
	ListCell   *l;
	Entry	   *cur_element;

	foreach(l, objects)
	{
		cur_element = (Entry *) lfirst(l);
		if (cur_element->clause_hash == clause_hash &&
			cur_element->relid == relid &&
			cur_element->global_relid == global_relid)
		{
			return;
		}
	}

	cur_element = palloc(sizeof(*cur_element));
	cur_element->clause_hash = clause_hash;
	cur_element->relid = relid;
	cur_element->global_relid = global_relid;
	cur_element->selectivity = selectivity;
	objects = lappend(objects, cur_element);
}

/*
 * Restores selectivity for given clause_hash and global_relid.
 */
double *
selectivity_cache_find_global_relid(int clause_hash, int global_relid)
{
	ListCell   *l;
	Entry	   *cur_element;

	foreach(l, objects)
	{
		cur_element = (Entry *) lfirst(l);
		if (cur_element->clause_hash == clause_hash &&
			cur_element->global_relid == global_relid)
		{
			return &(cur_element->selectivity);
		}
	}
	return NULL;
}

/*
 * Clears selectivity cache.
 */
void
selectivity_cache_clear(void)
{
	MemoryContextReset(AQO_cache_mem_ctx);
	objects = NIL;
}
