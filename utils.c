#include "aqo.h"

/*****************************************************************************
 *
 *	UTILITIES
 *
 *****************************************************************************/

/* TODO: get rid of those static vars */
static void *argsort_a;
static size_t argsort_es;
static int	(*argsort_value_cmp) (const void *, const void *);

static int	argsort_cmp(const void *a, const void *b);


/*
 * Function for qsorting an integer arrays
 */
int
int_cmp(const void *a, const void *b)
{
	if (*(int *) a < *(int *) b)
		return -1;
	else if (*(int *) a > *(int *) b)
		return 1;
	else
		return 0;
}

/*
 * Function for qsorting an double arrays
 */
int
double_cmp(const void *a, const void *b)
{
	if (*(double *) a < *(double *) b)
		return -1;
	else if (*(double *) a > *(double *) b)
		return 1;
	else
		return 0;
}

/*
 * Compares elements for two given indexes
 */
int
argsort_cmp(const void *a, const void *b)
{
	return (*argsort_value_cmp) ((char *) argsort_a +
								 *((int *) a) * argsort_es,
								 (char *) argsort_a +
								 *((int *) b) * argsort_es);
}

/*
 * Returns array of indexes that makes given array sorted.
 */
int *
argsort(void *a, int n, size_t es, int (*cmp) (const void *, const void *))
{
	int		   *idx = palloc(n * sizeof(*idx));
	int			i;

	for (i = 0; i < n; ++i)
		idx[i] = i;
	argsort_value_cmp = cmp;
	argsort_a = a;
	argsort_es = es;

	/* TODO: replace with qsort_arg() + see vars at the top */
	qsort(idx, n, sizeof(*idx), argsort_cmp);

	return idx;
}

/*
 * Returns the inverse of given permutation.
 */
int *
inverse_permutation(int *idx, int n)
{
	int		   *inv = palloc(n * sizeof(*inv));
	int			i;

	for (i = 0; i < n; ++i)
		inv[idx[i]] = i;
	return inv;
}

/*
 * Allocates empty QueryStat object.
 */
QueryStat *
palloc_query_stat(void)
{
	QueryStat  		*res;
	MemoryContext	oldCxt;

	oldCxt = MemoryContextSwitchTo(AQOMemoryContext);
	res = palloc0(sizeof(*res));
	res->execution_time_with_aqo = palloc(aqo_stat_size *
								sizeof(res->execution_time_with_aqo[0]));
	res->execution_time_without_aqo = palloc(aqo_stat_size *
								sizeof(res->execution_time_without_aqo[0]));
	res->planning_time_with_aqo = palloc(aqo_stat_size *
								sizeof(res->planning_time_with_aqo[0]));
	res->planning_time_without_aqo = palloc(aqo_stat_size *
								sizeof(res->planning_time_without_aqo[0]));
	res->cardinality_error_with_aqo = palloc(aqo_stat_size *
								sizeof(res->cardinality_error_with_aqo[0]));
	res->cardinality_error_without_aqo = palloc(aqo_stat_size *
								sizeof(res->cardinality_error_without_aqo[0]));
	MemoryContextSwitchTo(oldCxt);

	return res;
}

/*
 * Frees QueryStat object.
 */
void
pfree_query_stat(QueryStat * stat)
{
	pfree(stat->execution_time_with_aqo);
	pfree(stat->execution_time_without_aqo);
	pfree(stat->planning_time_with_aqo);
	pfree(stat->planning_time_without_aqo);
	pfree(stat->cardinality_error_with_aqo);
	pfree(stat->cardinality_error_without_aqo);
	pfree(stat);
}
