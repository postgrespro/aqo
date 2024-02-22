/*
 *******************************************************************************
 *
 *	UTILITIES
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/utils.c
 *
 */

#include "postgres.h"

#include "aqo.h"

/* TODO: get rid of those static vars */
static void *argsort_a;
static size_t argsort_es;
static int	(*argsort_value_cmp) (const void *, const void *);

static int	argsort_cmp(const void *a, const void *b);


/*
 * qsort comparator functions
 */

/*
 * Function for qsorting an integer arrays
 */
int
int_cmp(const void *arg1, const void *arg2)
{
	int			v1 = *((const int *) arg1);
	int			v2 = *((const int *) arg2);

	if (v1 < v2)
		return -1;
	else if (v1 > v2)
		return 1;
	else
		return 0;
}

/*
 * Function for qsorting an double arrays
 */
int
double_cmp(const void *arg1, const void *arg2)
{
	double		v1 = *((const double *) arg1);
	double		v2 = *((const double *) arg2);

	if (v1 < v2)
		return -1;
	else if (v1 > v2)
		return 1;
	else
		return 0;
}

/*
 * Compares elements for two given indexes
 */
int
argsort_cmp(const void *arg1, const void *arg2)
{
	int			idx1 = *((const int *) arg1);
	int			idx2 = *((const int *) arg2);
	char	   *arr = (char *) argsort_a;

	return (*argsort_value_cmp) (&arr[idx1 * argsort_es],
								 &arr[idx2 * argsort_es]);
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
