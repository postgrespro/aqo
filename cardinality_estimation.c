#include "aqo.h"
#include "optimizer/optimizer.h"

/*****************************************************************************
 *
 *	CARDINALITY ESTIMATION
 *
 * This is the module in which cardinality estimation problem obtained from
 * cardinality_hooks turns into machine learning problem.
 *
 *****************************************************************************/

/*
 * General method for prediction the cardinality of given relation.
 */
double
predict_for_relation(List *restrict_clauses, List *selectivities, List *relids, int *fss_hash)
{
	int		nfeatures;
	double	*matrix[aqo_K];
	double	targets[aqo_K];
	double	*features;
	double	result;
	int		rows;
	int		i;

	*fss_hash = get_fss_for_object(restrict_clauses, selectivities, relids,
														&nfeatures, &features);

	if (nfeatures > 0)
		for (i = 0; i < aqo_K; ++i)
			matrix[i] = palloc0(sizeof(**matrix) * nfeatures);

	if (load_fss(*fss_hash, nfeatures, matrix, targets, &rows))
		result = OkNNr_predict(rows, nfeatures, matrix, targets, features);
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

	pfree(features);
	if (nfeatures > 0)
	{
		for (i = 0; i < aqo_K; ++i)
			pfree(matrix[i]);
	}

	if (result < 0)
		return -1;
	else
		return clamp_row_est(exp(result));
}
