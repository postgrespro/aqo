# contrib/aqo/Makefile

EXTENSION = aqo
EXTVERSION = 1.2
PGFILEDESC = "AQO - adaptive query optimization"
MODULES = aqo
OBJS = aqo.o auto_tuning.o cardinality_estimation.o cardinality_hooks.o \
hash.o machine_learning.o path_utils.o postprocessing.o preprocessing.o \
selectivity_cache.o storage.o utils.o $(WIN32RES)

REGRESS =	aqo_disabled \
			aqo_controlled \
			aqo_intelligent \
			aqo_forced \
			aqo_learn \
			schema

EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/conf.add

DATA = aqo--1.0.sql aqo--1.0--1.1.sql aqo--1.1--1.2.sql
DATA_built = aqo--1.2.sql

MODULE_big = aqo
ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/aqo
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif


$(DATA_built): $(DATA)
	cat $+ > $@
