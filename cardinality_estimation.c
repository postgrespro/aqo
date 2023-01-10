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
#include "storage.h"


bool use_wide_search = false;

#ifdef AQO_DEBUG_PRINT
static void
predict_debug_output(List *clauses, List *selectivities,
					 List *reloids, int fss, double result)
{
	StringInfoData debug_str;
	ListCell *lc;

	initStringInfo(&debug_str);
	appendStringInfo(&debug_str, "fss: %d, clausesNum: %d, ",
					 fss, list_length(clauses));

	appendStringInfoString(&debug_str, ", selectivities: { ");
	foreach(lc, selectivities)
	{
		Selectivity *s = (Selectivity *) lfirst(lc);
		appendStringInfo(&debug_str, "%lf ", *s);
	}

	appendStringInfoString(&debug_str, "}, reloids: { ");
	foreach(lc, reloids)
	{
		Oid relname = lfirst_oid(lc);
		appendStringInfo(&debug_str, "%d ", relname);
	}

	appendStringInfo(&debug_str, "}, result: %lf", result);
	elog(DEBUG1, "Prediction: %s", debug_str.data);
}
#endif

/*
 * General method for prediction the cardinality of given relation.
 */
double
predict_for_relation(List *clauses, List *selectivities, List *relsigns,
					 int *fss)
{
	double	   *features;
	double		result;
	int			ncols;
	OkNNrdata  *data;

	if (relsigns == NIL)
		/*
		 * Don't make prediction for query plans without any underlying plane
		 * tables. Use return value -4 for debug purposes.
		 */
		return -4.;

	*fss = get_fss_for_object(relsigns, clauses, selectivities,
							  &ncols, &features);
	data = OkNNr_allocate(ncols);

	if (load_fss_ext(query_context.fspace_hash, *fss, data, NULL))
		result = OkNNr_predict(data, features);
	else
	{
		/*
		 * Due to planning optimizer tries to build many alternate paths. Many
		 * of them aren't used in final query execution path. Consequently, only
		 * small part of paths was used for AQO learning and stored into
		 * the AQO knowledge base.
		 */

		/* Try to search in surrounding feature spaces for the same node */
		if (!load_aqo_data(query_context.fspace_hash, *fss, data, NULL, use_wide_search))
			result = -1;
		else
		{
			elog(DEBUG5, "[AQO] Make prediction for fss %d by a neighbour "
				 "includes %d feature(s) and %d fact(s).",
				 *fss, data->cols, data->rows);
			result = OkNNr_predict(data, features);
		}
	}

#ifdef AQO_DEBUG_PRINT
	predict_debug_output(clauses, selectivities, relsigns, *fss, result);
#endif

	if (result < 0)
		return -1;
	else
		return clamp_row_est(exp(result));
}
