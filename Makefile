# contrib/aqo/Makefile

EXTENSION = aqo
EXTVERSION = 1.6
PGFILEDESC = "AQO - Adaptive Query Optimization"
MODULE_big = aqo
OBJS = $(WIN32RES) \
	aqo.o auto_tuning.o cardinality_estimation.o cardinality_hooks.o \
	hash.o machine_learning.o path_utils.o postprocessing.o preprocessing.o \
	selectivity_cache.o storage.o utils.o learn_cache.o aqo_shared.o

TAP_TESTS = 1

# Use an empty dummy test to define the variable REGRESS and therefore run all
# regression tests. regress_schedule contains the full list of real tests.
REGRESS = aqo_dummy_test
REGRESS_OPTS = --schedule=$(srcdir)/regress_schedule

fdw_srcdir = $(top_srcdir)/contrib/postgres_fdw
stat_srcdir = $(top_srcdir)/contrib/pg_stat_statements
PG_CPPFLAGS += -I$(libpq_srcdir) -I$(fdw_srcdir) -I$(stat_srcdir)
EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/aqo.conf
EXTRA_INSTALL = contrib/postgres_fdw contrib/pg_stat_statements

DATA = aqo--1.0.sql aqo--1.0--1.1.sql aqo--1.1--1.2.sql aqo--1.2.sql \
		aqo--1.2--1.3.sql aqo--1.3--1.4.sql aqo--1.4--1.5.sql \
		aqo--1.5--1.6.sql

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
