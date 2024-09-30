-- Preliminaries
CREATE EXTENSION IF NOT EXISTS aqo;
SELECT true AS success FROM aqo_reset();

-- Utility tool. Allow to filter system-dependent strings from an explain output.
CREATE OR REPLACE FUNCTION expln(query_string text) RETURNS SETOF text AS $$
BEGIN
    RETURN QUERY
        EXECUTE format('%s', query_string);
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

SET aqo.mode = 'learn';
SET aqo.show_details = true;
SET compute_query_id = 'auto';

CREATE TABLE t(x int);
INSERT INTO t (x) (SELECT * FROM generate_series(1, 100) AS gs);
ANALYZE t;

SELECT true AS success FROM aqo_reset();
-- Check AQO addons to explain (the only stable data)
SELECT regexp_replace(
        str,'Query Identifier: -?\m\d+\M','Query Identifier: N','g') as str FROM expln('
  EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
    SELECT x FROM t;
') AS str;
SELECT regexp_replace(
        str,'Query Identifier: -?\m\d+\M','Query Identifier: N','g') as str FROM expln('
  EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
    SELECT x FROM t;
') AS str;
SET aqo.mode = 'disabled';

-- Check existence of the interface functions.
SELECT obj_description('aqo_cardinality_error'::regproc::oid);
SELECT obj_description('aqo_execution_time'::regproc::oid);
SELECT obj_description('aqo_drop_class'::regproc::oid);
SELECT obj_description('aqo_cleanup'::regproc::oid);
SELECT obj_description('aqo_reset'::regproc::oid);

\df aqo_cardinality_error
\df aqo_execution_time
\df aqo_drop_class
\df aqo_cleanup
\df aqo_reset

-- Check stat reset
SELECT count(*) FROM aqo_query_stat;
SELECT true AS success FROM aqo_reset();
SELECT count(*) FROM aqo_query_stat;

DROP TABLE t;
DROP EXTENSION aqo;
