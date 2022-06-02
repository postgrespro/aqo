#ifndef STORAGE_H
#define STORAGE_H

#include "aqo.h"

extern bool aqo_use_file_storage;

extern void aqo_store_stat(uint64 queryid, QueryStat * stat);
extern QueryStat *aqo_load_stat(uint64 queryid);
#endif /* STORAGE_H */
