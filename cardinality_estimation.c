#include "aqo.h"

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
predict_for_relation(List *restrict_clauses, List *selectivities, List *relids)
{
	int			nfeatures;
	int			fss_hash;
	double	  **matrix;
	double	   *target;
	double	   *features;
	double		result;
	int			rows;
	int			i;
	ListCell   *l;

	get_fss_for_object(restrict_clauses, selectivities, relids,
					   &nfeatures, &fss_hash, &features);

	matrix = palloc(sizeof(*matrix) * aqo_K);
	for (i = 0; i < aqo_K; ++i)
		matrix[i] = palloc0(sizeof(**matrix) * nfeatures);
	target = palloc0(sizeof(*target) * aqo_K);

	if (load_fss(fss_hash, nfeatures, matrix, target, &rows))
		result = OkNNr_predict(rows, nfeatures, matrix, target, features);
	else
		result = -1;

	pfree(features);
	for (i = 0; i < aqo_K; ++i)
		pfree(matrix[i]);
	pfree(matrix);
	pfree(target);
	list_free(restrict_clauses);
	foreach(l, selectivities)
		pfree(lfirst(l));
	list_free(selectivities);
	list_free(relids);

	if (result < 0)
		return -1;
	else
		return exp(result);
}
