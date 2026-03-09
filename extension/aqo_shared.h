#ifndef AQO_SHARED_H
#define AQO_SHARED_H

#include "storage/lwlock.h"
#include "utils/dsa.h"

#define AQO_SHARED_MAGIC	0x053163

typedef struct AQOSharedState
{
	LWLock		lock;			/* mutual exclusion */

	/* Storage fields */
	LWLock		stat_lock; /* lock for access to stat storage */
	bool		stat_changed;

	LWLock		qtexts_lock; /* Lock for shared fields below */
	dsa_handle	qtexts_dsa_handler; /* DSA area for storing of query texts */
	int			qtext_trancheid;
	bool		qtexts_changed;

	LWLock		data_lock; /* Lock for shared fields below */
	dsa_handle	data_dsa_handler;
	bool		data_changed;

	LWLock		queries_lock;  /* lock for access to queries storage */
	bool		queries_changed;
} AQOSharedState;


extern AQOSharedState *aqo_state;

extern int fs_max_items; /* Max number of feature spaces that AQO can operate */
extern int fss_max_items;

extern void aqo_shmem_init(void);

#endif /* AQO_SHARED_H */
