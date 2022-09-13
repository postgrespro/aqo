/*
 *******************************************************************************
 *
 *	CARDINALITY ESTIMATION
 *
 * This is the module in which cardinality estimation problem obtained from
 * cardinality_hooks turns into machine learning problem.
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/cardinality_estimation.c
 *
 */

#include "postgres.h"

#include "optimizer/optimizer.h"

#include "aqo.h"
#include "hash.h"
#include "machine_learning.h"

#ifdef AQO_DEBUG_PRINT
static void
predict_debug_output(List *clauses, List *selectivities,
					 List *relids, int fss_hash, double result)
{
	StringInfoData debug_str;
	ListCell *lc;

	initStringInfo(&debug_str);
	appendStringInfo(&debug_str, "fss: %d, clausesNum: %d, ",
					 fss_hash, list_length(clauses));

	appendStringInfoString(&debug_str, ", selectivities: { ");
	foreach(lc, selectivities)
	{
		Selectivity *s = (Selectivity *) lfirst(lc);
		appendStringInfo(&debug_str, "%lf ", *s);
	}

	appendStringInfoString(&debug_str, "}, relids: { ");
	foreach(lc, relids)
	{
		int relid = lfirst_int(lc);
		appendStringInfo(&debug_str, "%d ", relid);
	}

	appendStringInfo(&debug_str, "}, result: %lf", result);
	elog(DEBUG1, "Prediction: %s", debug_str.data);
	pfree(debug_str.data);
}
#endif

/*
 * General method for prediction the cardinality of given relation.
 */
double
predict_for_relation(List *clauses, List *selectivities,
					 List *relids, int *fss)
{
	double	   *features;
	double		result;
	int			i;
	OkNNrdata	data;

	if (relids == NIL)
		/*
		 * Don't make prediction for query plans without any underlying plane
		 * tables. Use return value -4 for debug purposes.
		 */
		return -4.;

	*fss = get_fss_for_object(relids, clauses,
								   selectivities, &data.cols, &features);

	if (data.cols > 0)
		for (i = 0; i < aqo_K; ++i)
			data.matrix[i] = palloc0(sizeof(double) * data.cols);

	if (load_fss_ext(query_context.fspace_hash, *fss, &data, NULL, true))
		result = OkNNr_predict(&data, features);
	else
	{
		/*
		 * Due to planning optimizer tries to build many alternate paths. Many
		 * of these not used in final query execution path. Consequently, only
		 * small part of paths was used for AQO learning and fetch into the AQO
		 * knowledge base.
		 */
		result = -1;
	}
#ifdef AQO_DEBUG_PRINT
	predict_debug_output(clauses, selectivities, relids, *fss_hash, result);
#endif
	pfree(features);
	if (data.cols > 0)
	{
		for (i = 0; i < aqo_K; ++i)
			pfree(data.matrix[i]);
	}

	if (result < 0)
		return -1;
	else
		return clamp_row_est(exp(result));
}
