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
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/machine_learning.c
 *
 */

#include "postgres.h"

#include "aqo.h"
#include "machine_learning.h"


/*
 * This parameter tell us that the new learning sample object has very small
 * distance from one whose features stored in matrix already.
 * In this case we will not to add new line in matrix, but will modify this
 * nearest neighbor features and cardinality with linear smoothing by
 * learning_rate coefficient.
 */
const double	object_selection_threshold = 0.1;
const double	learning_rate = 1e-1;


static double fs_distance(double *a, double *b, int len);
static double fs_similarity(double dist);
static double compute_weights(double *distances, int nrows, double *w, int *idx);


OkNNrdata*
OkNNr_allocate(int ncols)
{
	OkNNrdata  *data = palloc(sizeof(OkNNrdata));
	int			i;

	if (ncols > 0)
		for (i = 0; i < aqo_K; i++)
			data->matrix[i] = palloc0(ncols * sizeof(double));
	else
		for (i = 0; i < aqo_K; i++)
			data->matrix[i] = NULL;

	data->cols = ncols;
	data->rows  = -1;
	return data;
}

/*
 * Computes L2-distance between two given vectors.
 */
static double
fs_distance(double *a, double *b, int len)
{
	double		res = 0;
	int			i;

	for (i = 0; i < len; ++i)
	{
		Assert(!isnan(a[i]));
		res += (a[i] - b[i]) * (a[i] - b[i]);
	}
	if (len != 0)
		res = sqrt(res);
	return res;
}

/*
 * Returns similarity between objects based on distance between them.
 */
static double
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
static double
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
OkNNr_predict(OkNNrdata *data, double *features)
{
	double	distances[aqo_K];
	int		i;
	int		idx[aqo_K]; /* indexes of nearest neighbors */
	double	w[aqo_K];
	double	w_sum;
	double	result = 0.;

	Assert(data != NULL);

	if (!aqo_predict_with_few_neighbors && data->rows < aqo_k)
		return -1.;

	for (i = 0; i < data->rows; ++i)
		distances[i] = fs_distance(data->matrix[i], features, data->cols);

	w_sum = compute_weights(distances, data->rows, w, idx);

	for (i = 0; i < aqo_k; ++i)
		if (idx[i] != -1)
			result += data->targets[idx[i]] * w[i] / w_sum;

	if (result < 0.)
		result = 0.;

	/* this should never happen */
	if (idx[0] == -1)
		result = -1.;

	return result;
}

/*
 * Modifies given matrix and targets using features and target value of new
 * object.
 * Returns indexes of changed lines: if index of line is less than matrix_rows
 * updates this line in database, otherwise adds new line with given index.
 * It is supposed that indexes of new lines are consequent numbers
 * starting from matrix_rows.
 * reliability: 1 - value after normal end of a query; 0.1 - data from partially
 * executed node (we don't want this part); 0.9 - from finished node, but
 * partially executed statement.
 */
int
OkNNr_learn(OkNNrdata *data, double *features, double target, double rfactor)
{
	double	distances[aqo_K];
	int		i;
	int		j;
	int		mid = 0; /* index of row with minimum distance value */
	int		idx[aqo_K];

	/*
	 * For each neighbor compute distance and search for nearest object.
	 */
	for (i = 0; i < data->rows; ++i)
	{
		distances[i] = fs_distance(data->matrix[i], features, data->cols);
		if (distances[i] < distances[mid])
			mid = i;
	}

	/*
	 * We do not want to add new very similar neighbor. And we can't
	 * replace data for the neighbor to avoid some fluctuations.
	 * We will change it's row with linear smoothing by learning_rate.
	 */
	if (data->rows > 0 && distances[mid] < object_selection_threshold)
	{
		double lr = learning_rate * rfactor / data->rfactors[mid];

		if (lr > 1.)
		{
			elog(WARNING, "[AQO] Something goes wrong in the ML core: learning rate = %lf", lr);
			lr = 1.;
		}

		Assert(lr > 0.);
		Assert(data->rfactors[mid] > 0. && data->rfactors[mid] <= 1.);

		for (j = 0; j < data->cols; ++j)
			data->matrix[mid][j] += lr * (features[j] - data->matrix[mid][j]);
		data->targets[mid] += lr * (target - data->targets[mid]);
		data->rfactors[mid] += lr * (rfactor - data->rfactors[mid]);

		return data->rows;
	}
	else if (data->rows < aqo_K)
	{
		/* We don't reach a limit of stored neighbors */

		/*
		 * Add new line into the matrix. We can do this because data->rows
		 * is not the boundary of matrix. Matrix has aqo_K free lines
		 */
		for (j = 0; j < data->cols; ++j)
			data->matrix[data->rows][j] = features[j];
		data->targets[data->rows] = target;
		data->rfactors[data->rows] = rfactor;

		return data->rows + 1;
	}
	else
	{
		double *feature;
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
		w_sum = compute_weights(distances, data->rows, w, idx);

		/*
		 * Compute average value for target by nearest neighbors. We need to
		 * check idx[i] != -1 because we may have smaller value of nearest
		 * neighbors than aqo_k.
		 * Semantics of tc_coef: it is defined distance between new object and
		 * this superposition value (with linear smoothing).
		 * fc_coef - feature changing rate.
		 * */
		for (i = 0; i < aqo_k && idx[i] != -1; ++i)
			avg_target += data->targets[idx[i]] * w[i] / w_sum;
		tc_coef = learning_rate * (avg_target - target);

		/* Modify targets and features of each nearest neighbor row. */
		for (i = 0; i < aqo_k && idx[i] != -1; ++i)
		{
			double lr = learning_rate * rfactor / data->rfactors[mid];

			if (lr > 1.)
			{
				elog(WARNING, "[AQO] Something goes wrong in the ML core: learning rate = %lf", lr);
				lr = 1.;
			}

			Assert(lr > 0.);
			Assert(data->rfactors[mid] > 0. && data->rfactors[mid] <= 1.);

			fc_coef = tc_coef * lr * (data->targets[idx[i]] - avg_target) *
										w[i] * w[i] / sqrt(data->cols) / w_sum;

			data->targets[idx[i]] -= tc_coef * lr * w[i] / w_sum;
			for (j = 0; j < data->cols; ++j)
			{
				feature = data->matrix[idx[i]];
				feature[j] -= fc_coef * (features[j] - feature[j]) /
					distances[idx[i]];
			}
		}
	}
	return data->rows;
}
