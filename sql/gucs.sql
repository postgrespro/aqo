CREATE EXTENSION aqo;
SET aqo.join_threshold = 0;

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
SELECT obj_description('aqo_cardinality_error'::regproc::oid);
SELECT obj_description('aqo_execution_time'::regproc::oid);
SELECT obj_description('aqo_drop_class'::regproc::oid);
SELECT obj_description('aqo_cleanup'::regproc::oid);
SELECT obj_description('aqo_reset_query'::regproc::oid);

\df aqo_cardinality_error
\df aqo_execution_time
\df aqo_drop_class
\df aqo_cleanup
\df aqo_reset_query

DROP EXTENSION aqo;
