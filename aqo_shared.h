#ifndef AQO_SHARED_H
#define AQO_SHARED_H


#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "lib/dshash.h"

#define AQO_SHARED_MAGIC	0x053163

typedef struct
{
	/* XXX we assume this struct contains no padding bytes */
	uint64	fs;
	int64	fss;
} htab_key;

typedef struct
{
	htab_key	key;
	uint32		hdr_off; /* offset of data in DSM cache */
} htab_entry;

typedef struct AQOSharedState
{
	LWLock		lock;			/* mutual exclusion */
	dsm_handle	dsm_handler;

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


extern shmem_startup_hook_type prev_shmem_startup_hook;
extern AQOSharedState *aqo_state;
extern HTAB *fss_htab;

extern int fs_max_items; /* Max number of feature spaces that AQO can operate */
extern int fss_max_items;

extern Size aqo_memsize(void);
extern void reset_dsm_cache(void);
extern void *get_dsm_all(uint32 *size);
extern char *get_cache_address(void);
extern uint32 get_dsm_cache_pos(uint32 size);
extern void aqo_init_shmem(void);

#endif /* AQO_SHARED_H */
