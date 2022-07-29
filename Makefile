# contrib/aqo/Makefile

EXTENSION = aqo
EXTVERSION = 1.5
PGFILEDESC = "AQO - Adaptive Query Optimization"
MODULE_big = aqo
OBJS = $(WIN32RES) \
	aqo.o auto_tuning.o cardinality_estimation.o cardinality_hooks.o \
	hash.o machine_learning.o path_utils.o postprocessing.o preprocessing.o \
	selectivity_cache.o storage.o utils.o learn_cache.o aqo_shared.o

TAP_TESTS = 1

REGRESS =	aqo_disabled \
			aqo_controlled \
			aqo_intelligent \
			aqo_forced \
			aqo_learn \
			schema \
			aqo_fdw \
			aqo_CVE-2020-14350 \
			gucs \
			forced_stat_collection \
			unsupported \
			clean_aqo_data \
			plancache	\
			statement_timeout \
			temp_tables \
			top_queries \
			relocatable\
			look_a_like \
			feature_subspace

fdw_srcdir = $(top_srcdir)/contrib/postgres_fdw
stat_srcdir = $(top_srcdir)/contrib/pg_stat_statements
PG_CPPFLAGS += -I$(libpq_srcdir) -I$(fdw_srcdir) -I$(stat_srcdir)
EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/conf.add
EXTRA_INSTALL = contrib/postgres_fdw contrib/pg_stat_statements

DATA = aqo--1.0.sql aqo--1.0--1.1.sql aqo--1.1--1.2.sql aqo--1.2.sql \
		aqo--1.2--1.3.sql aqo--1.3--1.4.sql aqo--1.4--1.5.sql

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
