# contrib/aqo/Makefile

MODULE_big = aqo
OBJS = aqo.o auto_tuning.o cardinality_estimation.o cardinality_hooks.o \
hash.o machine_learning.o path_utils.o postprocessing.o preprocessing.o \
selectivity_cache.o storage.o utils.o $(WIN32RES)
EXTENSION = aqo
DATA = aqo--1.0.sql
PGFILEDESC = "aqo - adaptive query optimization"

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
