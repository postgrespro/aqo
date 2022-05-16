CREATE EXTENSION aqo;
SET aqo.mode = 'learn';
SET aqo.show_details = true;

CREATE TABLE t(x int);
INSERT INTO t (x) (SELECT * FROM generate_series(1, 100) AS gs);
ANALYZE t;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
	SELECT x FROM t;
EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
	SELECT x FROM t;

-- Check existence of the interface functions.
SELECT obj_description('public.show_cardinality_errors'::regproc::oid);
SELECT obj_description('public.show_execution_time'::regproc::oid);

\df show_cardinality_errors
\df show_execution_time

DROP EXTENSION aqo;
