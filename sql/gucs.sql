-- Switch off parallel workers because of unsteadiness.
-- Do this in each aqo test separately, so that server regression tests pass
-- with aqo's temporary configuration file loaded.
SET max_parallel_workers TO 0;

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

DROP EXTENSION aqo;
