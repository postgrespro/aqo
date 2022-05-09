#ifndef MACHINE_LEARNING_H
#define MACHINE_LEARNING_H

/* Max number of matrix rows - max number of possible neighbors. */
#define	aqo_K	(30)

extern const double object_selection_threshold;
extern const double learning_rate;

#define RELIABILITY_MIN		(0.1)
#define RELIABILITY_MAX		(1.0)

typedef struct OkNNrdata
{
	int		rows; /* Number of filled rows in the matrix */
	int		cols; /* Number of columns in the matrix */

	double *matrix[aqo_K]; /* Contains the matrix - learning data for the same
							* value of (fs, fss), but different features. */
	double	targets[aqo_K]; /* Right side of the equations system */
	double	rfactors[aqo_K];
} OkNNrdata;

/* Machine learning techniques */
extern double OkNNr_predict(OkNNrdata *data, double *features);
extern int OkNNr_learn(OkNNrdata *data,
					   double *features, double target, double rfactor);
extern int get_avg_over_neibours(OkNNrdata *data,
					   double *features);

#endif /* MACHINE_LEARNING_H */
