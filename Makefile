# contrib/aqo/Makefile

EXTENSION = aqo
EXTVERSION = 1.2
PGFILEDESC = "AQO - Adaptive Query Optimization"
MODULE_big = aqo
OBJS = aqo.o auto_tuning.o cardinality_estimation.o cardinality_hooks.o \
hash.o machine_learning.o path_utils.o postprocessing.o preprocessing.o \
selectivity_cache.o storage.o utils.o ignorance.o $(WIN32RES)

# Use PG_TEST_SKIP="...aqo..." to skip aqo tests if necessary.
ifneq (aqo,$(filter aqo,$(PG_TEST_SKIP)))
TAP_TESTS = 1

REGRESS =	aqo_disabled \
			aqo_controlled \
			aqo_intelligent \
			aqo_forced \
			aqo_learn \
			schema \
			aqo_fdw \
			aqo_CVE-2020-14350 \
			gucs
endif

fdw_srcdir = $(top_srcdir)/contrib/postgres_fdw
PG_CPPFLAGS += -I$(libpq_srcdir) -I$(fdw_srcdir)
EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/conf.add
EXTRA_CLEAN = $(pg_regress_clean_files) sql/tablespace.sql \
	sql/misc.sql sql/largeobject.sql sql/create_function_2.sql \
	sql/create_function_1.sql sql/copy.sql sql/constraints.sql \
	expected/tablespace.out \
	expected/misc.out expected/largeobject.out expected/largeobject_1.out \
	expected/create_function_2.out expected/create_function_1.out \
	expected/copy.out expected/copy_1.out expected/constraints.out
EXTRA_INSTALL = contrib/postgres_fdw

DATA = aqo--1.0.sql aqo--1.0--1.1.sql aqo--1.1--1.2.sql aqo--1.2.sql

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

aqo-regress:
	$(pg_regress_check) \
	--dlpath=$(CURDIR)/$(top_builddir)/src/test/regress \
	--inputdir=$(abs_top_srcdir)/src/test/regress \
	--schedule=$(CURDIR)/parallel_schedule \
	--load-extension=aqo
