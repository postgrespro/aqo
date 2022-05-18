CREATE EXTENSION aqo;

-- Check interface variables and their default values. Detect, if default value
-- of a GUC is changed.
SHOW aqo.join_threshold;
SHOW aqo.learn_statement_timeout;
SHOW aqo.show_hash;
SHOW  aqo.show_details;
SHOW aqo.force_collect_stat;
SHOW  aqo.mode;

SET aqo.mode = 'learn';
SET aqo.show_details = true;

CREATE TABLE t(x int);
INSERT INTO t (x) (SELECT * FROM generate_series(1, 100) AS gs);
ANALYZE t;

-- Check AQO addons to explain (the only stable data)
EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
	SELECT x FROM t;
EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
	SELECT x FROM t;

-- Check existence of the interface functions.
SELECT obj_description('public.show_cardinality_errors'::regproc::oid);
SELECT obj_description('public.show_execution_time'::regproc::oid);
SELECT obj_description('public.aqo_drop_class'::regproc::oid);
SELECT obj_description('public.aqo_cleanup'::regproc::oid);

\df show_cardinality_errors
\df show_execution_time
\df aqo_drop_class
\df aqo_cleanup

DROP EXTENSION aqo;
