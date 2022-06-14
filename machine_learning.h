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

extern OkNNrdata* OkNNr_allocate(int ncols);
extern void OkNNr_free(OkNNrdata *data);

/* Machine learning techniques */
extern double OkNNr_predict(OkNNrdata *data, double *features);
extern int OkNNr_learn(OkNNrdata *data,
					   double *features, double target, double rfactor);

#endif /* MACHINE_LEARNING_H */
