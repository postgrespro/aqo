#include "aqo.h"

/*****************************************************************************
 *
 *	AUTOMATIC QUERY TUNING
 *
 * This module automatically implements basic strategies of tuning AQO for best
 * PostgreSQL performance.
 *
 *****************************************************************************/

static double get_mean(double *elems, int nelems);
static double get_estimation(double *elems, int nelems);
static bool is_stable(double *elems, int nelems);
static bool converged_cq(double *elems, int nelems);
static bool is_in_infinite_loop_cq(double *elems, int nelems);


/*
 * Returns mean value of the array of doubles.
 */
double
get_mean(double *elems, int nelems)
{
	double	sum = 0;
	int		i;

	AssertArg(nelems > 0);

	for (i = 0; i < nelems; ++i)
		sum += elems[i];
	return sum / nelems;
}

/*
 * Having a time series it tries to predict its next value.
 * Now it do simple window averaging.
 */
double
get_estimation(double *elems, int nelems)
{
	int start;

	AssertArg(nelems > 0);

	if (nelems > auto_tuning_window_size)
		start = nelems - auto_tuning_window_size;
	else
		start = 0;

	return get_mean(&elems[start], nelems - start);
}

/*
 * Checks whether the series is stable with absolute or relative error 0.1.
 */
bool
is_stable(double *elems, int nelems)
{
	double	est,
			last;

	AssertArg(nelems > 1);

	est = get_mean(elems, nelems - 1);
	last = elems[nelems - 1];

	return (est * 1.1 > last || est + 0.1 > last) &&
		   (est * 0.9 < last || est - 0.1 < last);
}

/*
 * Tests whether cardinality qualities series is converged, i. e. learning
 * process may be considered as finished.
 * Now it checks whether the cardinality quality stopped decreasing with
 * absolute or relative error 0.1.
 */
bool
converged_cq(double *elems, int nelems)
{
	if (nelems < auto_tuning_window_size + 2)
		return false;

	return is_stable(&elems[nelems - auto_tuning_window_size - 1],
					 auto_tuning_window_size + 1);
}

/*
 * Tests whether cardinality qualities series is converged, i. e. learning
 * process may be considered as finished.
 * Now it checks whether the cardinality quality stopped decreasing with
 * absolute or relative error 0.1.
 */
bool
is_in_infinite_loop_cq(double *elems, int nelems)
{
	if (nelems - auto_tuning_infinite_loop < auto_tuning_window_size + 2)
		return false;

	return !converged_cq(elems, nelems) &&
		   !converged_cq(elems, nelems - auto_tuning_window_size);
}

/*
 * Here we use execution statistics for the given query tuning. Note that now
 * we cannot execute queries on our own wish, so the tuning now is in setting
 * use_aqo and learn_aqo parameters for the query type.
 *
 * Now the workflow is quite simple:
 *
 * Firstly, we run a new query type auto_tuning_window_size times without our
 * method to have an execution time statistics for such type of queries.
 * Secondly, we run the query type with both AQO usage and AQO learning enabled
 * until convergence.
 *
 * If AQO provides better execution time for the query type according to
 * collected statistics, we prefer to enable it, otherwise we prefer to disable
 * it.
 * In the stable workload case we perform an exploration. That means that with
 * some probability which depends on execution time with and without using AQO
 * we run the slower method to check whether it remains slower.
 * Cardinality statistics collection is enabled by default in this mode.
 * If we find out that cardinality quality diverged during the exploration, we
 * return to step 2 and run the query type with both AQO usage and AQO learning
 * enabled until convergence.
 * If after auto_tuning_max_iterations steps we see that for this query
 * it is better not to use AQO, we set auto_tuning, learn_aqo and use_aqo for
 * this query to false.
 */
void
automatical_query_tuning(int query_hash, QueryStat * stat)
{
	double		unstability = auto_tuning_exploration;
	double		t_aqo,
				t_not_aqo;
	double		p_use = -1;
	int64		num_iterations;

	num_iterations = stat->executions_with_aqo + stat->executions_without_aqo;
	query_context.learn_aqo = true;
	if (stat->executions_without_aqo < auto_tuning_window_size + 1)
		query_context.use_aqo = false;
	else if (!converged_cq(stat->cardinality_error_with_aqo,
						   stat->cardinality_error_with_aqo_size) &&
			 !is_in_infinite_loop_cq(stat->cardinality_error_with_aqo,
									 stat->cardinality_error_with_aqo_size))
		query_context.use_aqo = true;
	else
	{
		t_aqo = get_estimation(stat->execution_time_with_aqo,
							   stat->execution_time_with_aqo_size) +
			get_estimation(stat->planning_time_with_aqo,
						   stat->planning_time_with_aqo_size);

		t_not_aqo = get_estimation(stat->execution_time_without_aqo,
								   stat->execution_time_without_aqo_size) +
			get_estimation(stat->planning_time_without_aqo,
						   stat->planning_time_without_aqo_size);

		p_use = t_not_aqo / (t_not_aqo + t_aqo);
		p_use = 1 / (1 + exp((p_use - 0.5) / unstability));
		p_use -= 1 / (1 + exp(-0.5 / unstability));
		p_use /= 1 - 2 / (1 + exp(-0.5 / unstability));

		/* borrowed from drandom() in float.c */
		query_context.use_aqo = (random() / ((double) MAX_RANDOM_VALUE + 1)) < p_use;
		query_context.learn_aqo = query_context.use_aqo;
	}

	if (num_iterations <= auto_tuning_max_iterations || p_use > 0.5)
		update_query(query_hash, query_context.learn_aqo, query_context.use_aqo, query_context.fspace_hash, true);
	else
		update_query(query_hash, false, false, query_context.fspace_hash, false);
}
