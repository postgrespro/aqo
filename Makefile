# contrib/aqo/Makefile

EXTENSION = aqo
PGFILEDESC = "AQO - adaptive query optimization"
MODULES = aqo
DATA = aqo--1.0.sql
OBJS = aqo.o auto_tuning.o cardinality_estimation.o cardinality_hooks.o \
hash.o machine_learning.o path_utils.o postprocessing.o preprocessing.o \
selectivity_cache.o storage.o utils.o $(WIN32RES)
REGRESS = aqo_disabled aqo_controlled aqo_intelligent aqo_forced

MODULE_big = aqo
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/aqo
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
