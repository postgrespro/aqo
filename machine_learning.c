/*
 *******************************************************************************
 *
 *	MACHINE LEARNING TECHNIQUES
 *
 * This module does not know anything about DBMS, cardinalities and all other
 * stuff. It learns matrices, predicts values and is quite happy.
 * The proposed method is designed for working with limited number of objects.
 * It is guaranteed that number of rows in the matrix will not exceed aqo_K
 * setting after learning procedure. This property also allows to adapt to
 * workloads which properties are slowly changed.
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2020, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/machine_learning.c
 *
 */

#include "aqo.h"

static double fs_distance(double *a, double *b, int len);
static double fs_similarity(double dist);
static double compute_weights(double *distances, int nrows, double *w, int *idx);


/*
 * Computes L2-distance between two given vectors.
 */
double
fs_distance(double *a, double *b, int len)
{
	double		res = 0;
	int			i;

	for (i = 0; i < len; ++i)
		res += (a[i] - b[i]) * (a[i] - b[i]);
	if (len != 0)
		res = sqrt(res / len);
	return res;
}

/*
 * Returns similarity between objects based on distance between them.
 */
double
fs_similarity(double dist)
{
	return 1.0 / (0.001 + dist);
}

/*
 * Compute weights necessary for both prediction and learning.
 * Creates and returns w, w_sum and idx based on given distances ad matrix_rows.
 *
 * Appeared as a separate function because of "don't repeat your code"
 * principle.
 */
double
compute_weights(double *distances, int nrows, double *w, int *idx)
{
	int		i,
			j;
	int		to_insert,
			tmp;
	double	w_sum = 0;

	for (i = 0; i < aqo_k; ++i)
		idx[i] = -1;

	/* Choose from all neighbors only several nearest objects */
	for (i = 0; i < nrows; ++i)
		for (j = 0; j < aqo_k; ++j)
			if (idx[j] == -1 || distances[i] < distances[idx[j]])
			{
				to_insert = i;
				for (; j < aqo_k; ++j)
				{
					tmp = idx[j];
					idx[j] = to_insert;
					to_insert = tmp;
				}
				break;
			}

	/* Compute weights by the nearest neighbors distances */
	for (j = 0; j < aqo_k && idx[j] != -1; ++j)
	{
		w[j] = fs_similarity(distances[idx[j]]);
		w_sum += w[j];
	}
	return w_sum;
}

/*
 * With given matrix, targets and features makes prediction for current object.
 *
 * Returns negative value in the case of refusal to make a prediction, because
 * positive targets are assumed.
 */
double
OkNNr_predict(int nrows, int ncols, double **matrix, const double *targets,
			  double *features)
{
	double	distances[aqo_K];
	int		i;
	int		idx[aqo_K]; /* indexes of nearest neighbors */
	double	w[aqo_K];
	double	w_sum;
	double	result = 0;

	for (i = 0; i < nrows; ++i)
		distances[i] = fs_distance(matrix[i], features, ncols);

	w_sum = compute_weights(distances, nrows, w, idx);

	for (i = 0; i < aqo_k; ++i)
		if (idx[i] != -1)
			result += targets[idx[i]] * w[i] / w_sum;

	if (result < 0)
		result = 0;

	/* this should never happen */
	if (idx[0] == -1)
		result = -1;

	return result;
}

/*
 * Modifies given matrix and targets using features and target value of new
 * object.
 * Returns indexes of changed lines: if index of line is less than matrix_rows
 * updates this line in database, otherwise adds new line with given index.
 * It is supposed that indexes of new lines are consequent numbers
 * starting from matrix_rows.
 */
int
OkNNr_learn(int nrows, int nfeatures, double **matrix, double *targets,
			double *features, double target)
{
	double	   distances[aqo_K];
	int			i,
				j;
	int			mid = 0; /* index of row with minimum distance value */
	int		   idx[aqo_K];

	/*
	 * For each neighbor compute distance and search for nearest object.
	 */
	for (i = 0; i < nrows; ++i)
	{
		distances[i] = fs_distance(matrix[i], features, nfeatures);
		if (distances[i] < distances[mid])
			mid = i;
	}

	/*
	 * We do not want to add new very similar neighbor. And we can't
	 * replace data for the neighbor to avoid some fluctuations.
	 * We will change it's row with linear smoothing by learning_rate.
	 */
	if (nrows > 0 && distances[mid] < object_selection_threshold)
	{
		for (j = 0; j < nfeatures; ++j)
			matrix[mid][j] += learning_rate * (features[j] - matrix[mid][j]);
		targets[mid] += learning_rate * (target - targets[mid]);

		return nrows;
	}

	if (nrows < aqo_K)
	{
		/* We can't reached limit of stored neighbors */

		/*
		 * Add new line into the matrix. We can do this because matrix_rows
		 * is not the boundary of matrix. Matrix has aqo_K free lines
		 */
		for (j = 0; j < nfeatures; ++j)
			matrix[nrows][j] = features[j];
		targets[nrows] = target;

		return nrows+1;
	}
	else
	{
		double	*feature;
		double	avg_target = 0;
		double	tc_coef; /* Target correction coefficient */
		double	fc_coef; /* Feature correction coefficient */
		double	w[aqo_K];
		double	w_sum;

		/*
		 * We reaches limit of stored neighbors and can't simply add new line
		 * at the matrix. Also, we can't simply delete one of the stored
		 * neighbors.
		 */

		/*
		 * Select nearest neighbors for the new object. store its indexes in
		 * idx array. Compute weight for each nearest neighbor and total weight
		 * of all nearest neighbor.
		 */
		w_sum = compute_weights(distances, nrows, w, idx);

		/*
		 * Compute average value for target by nearest neighbors. We need to
		 * check idx[i] != -1 because we may have smaller value of nearest
		 * neighbors than aqo_k.
		 * Semantics of coef1: it is defined distance between new object and
		 * this superposition value (with linear smoothing).
		 * */
		for (i = 0; i < aqo_k && idx[i] != -1; ++i)
			avg_target += targets[idx[i]] * w[i] / w_sum;
		tc_coef = learning_rate * (avg_target - target);

		/* Modify targets and features of each nearest neighbor row. */
		for (i = 0; i < aqo_k && idx[i] != -1; ++i)
		{
			fc_coef = tc_coef * (targets[idx[i]] - avg_target) * w[i] * w[i] /
				sqrt(nfeatures) / w_sum;

			targets[idx[i]] -= tc_coef * w[i] / w_sum;
			for (j = 0; j < nfeatures; ++j)
			{
				feature = matrix[idx[i]];
				feature[j] -= fc_coef * (features[j] - feature[j]) /
					distances[idx[i]];
			}
		}
	}

	return nrows;
}
