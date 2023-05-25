/*
 *******************************************************************************
 *
 *	AUTOMATIC QUERY TUNING
 *
 * This module automatically implements basic strategies of tuning AQO for best
 * PostgreSQL performance.
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2023, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/auto_tuning.c
 *
 */

#include "postgres.h"

#include "common/pg_prng.h"
#include "aqo.h"
#include "storage.h"

/*
 * Auto tuning criteria criteria of an query convergence by overall cardinality
 * of plan nodes.
 */
double auto_tuning_convergence_error = 0.01;

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

	Assert(nelems > 0);

	for (i = 0; i < nelems; ++i)
		sum += elems[i];
	return sum / nelems;
}

/*
 * Having a time series it tries to predict its next value.
 * Now it do simple window averaging.
 */
static double
get_estimation(double *elems, int nelems)
{
	int start;

	Assert(nelems > 0);

	if (nelems > auto_tuning_window_size)
		start = nelems - auto_tuning_window_size;
	else
		start = 0;

	return get_mean(&elems[start], nelems - start);
}

/*
 * Checks whether the series is stable with absolute or relative error.
 */
static bool
is_stable(double *elems, int nelems)
{
	double	est,
			last;

	Assert(nelems > 1);

	est = get_mean(elems, nelems - 1);
	last = elems[nelems - 1];

	return (est * (1. + auto_tuning_convergence_error) > last || est + auto_tuning_convergence_error > last) &&
		   (est * (1. - auto_tuning_convergence_error) < last || est - auto_tuning_convergence_error < last);
}

/*
 * Tests whether cardinality qualities series is converged, i. e. learning
 * process may be considered as finished.
 * Now it checks whether the cardinality quality stopped decreasing with
 * absolute or relative error.
 */
static bool
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
static bool
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
automatical_query_tuning(uint64 queryid, StatEntry *stat)
{
	double	unstability = auto_tuning_exploration;
	double	t_aqo,
			t_not_aqo;
	double	p_use = -1;
	int64	num_iterations;

	num_iterations = stat->execs_with_aqo + stat->execs_without_aqo;
	query_context.learn_aqo = true;
	if (stat->execs_without_aqo < auto_tuning_window_size + 1)
		query_context.use_aqo = false;
	else if (!converged_cq(stat->est_error_aqo, stat->cur_stat_slot_aqo) &&
			 !is_in_infinite_loop_cq(stat->est_error_aqo,
									 stat->cur_stat_slot_aqo))
		query_context.use_aqo = true;
	else
	{
		/*
		 * Query is converged by cardinality error. Now AQO checks convergence
		 * by execution time. It is volatile, probabilistic part of code.
		 * XXX: this logic of auto tuning may be reworked later.
		 */
		t_aqo = get_estimation(stat->exec_time_aqo, stat->cur_stat_slot_aqo) +
				get_estimation(stat->plan_time_aqo, stat->cur_stat_slot_aqo);

		t_not_aqo = get_estimation(stat->exec_time, stat->cur_stat_slot) +
					get_estimation(stat->plan_time, stat->cur_stat_slot);

		p_use = t_not_aqo / (t_not_aqo + t_aqo);

		/*
		 * Here p_use<0.5 and p_use->0, if AQO decreases performance,
		 * Otherwise, p_use>0.5 and p_use->1.
		 */

		p_use = 1 / (1 + exp((p_use - 0.5) / unstability));

		/*
		 * Here p_use in (0.5..max) if AQO decreases preformance.
		 * p_use in (0..0.5), otherwise.
		 */

		p_use -= 1 / (1 + exp(-0.5 / unstability));
		p_use /= 1 - 2 / (1 + exp(-0.5 / unstability));

		/*
		 * If our decision is using AQO for this query class, then learn on new
		 * queries of this type. Otherwise, turn off.
		 */
		query_context.use_aqo = pg_prng_double(&pg_global_prng_state) < p_use;
		query_context.learn_aqo = query_context.use_aqo;
	}

	if (num_iterations <= auto_tuning_max_iterations || p_use > 0.5)
		aqo_queries_store(queryid, query_context.fspace_hash,
						  query_context.learn_aqo, query_context.use_aqo, true,
						  &aqo_queries_nulls);
	else
		aqo_queries_store(queryid,
						  query_context.fspace_hash, false, false, false,
						  &aqo_queries_nulls);
}
