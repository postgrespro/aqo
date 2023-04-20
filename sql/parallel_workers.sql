-- Specifically test AQO machinery for queries uses partial paths and executed
-- with parallel workers.

CREATE EXTENSION IF NOT EXISTS aqo;
SELECT true AS success FROM aqo_reset();

-- Utility tool. Allow to filter system-dependent strings from explain output.
CREATE OR REPLACE FUNCTION expln(query_string text) RETURNS SETOF text AS $$
BEGIN
    RETURN QUERY
        EXECUTE format('%s', query_string);
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

SET aqo.mode = 'learn';
SET aqo.show_details = true;

-- Be generous with a number parallel workers to test the machinery
SET max_parallel_workers = 64;
SET max_parallel_workers_per_gather = 64;
-- Enforce usage of parallel workers
SET parallel_setup_cost = 0.1;
SET parallel_tuple_cost = 0.0001;

CREATE TABLE t AS (
  SELECT x AS id, repeat('a', 512) AS payload FROM generate_series(1, 1E5) AS x
);
ANALYZE t;

-- Simple test. Check serialization machinery mostly.
SELECT count(*) FROM t WHERE id % 100 = 0; -- Learning stage
SELECT str FROM expln('
  EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
    SELECT count(*) FROM t WHERE id % 100 = 0;') AS str
WHERE str NOT LIKE '%Worker%';

-- More complex query just to provoke errors
SELECT count(*) FROM
  (SELECT id FROM t WHERE id % 100 = 0 GROUP BY (id)) AS q1,
  (SELECT max(id) AS id, payload FROM t
    WHERE id % 101 = 0 GROUP BY (payload)) AS q2
WHERE q1.id = q2.id; -- Learning stage
-- XXX: Why grouping prediction isn't working here?
SELECT str FROM expln('
EXPLAIN (COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT count(*) FROM
  (SELECT id FROM t WHERE id % 100 = 0 GROUP BY (id)) AS q1,
  (SELECT max(id) AS id, payload FROM t
    WHERE id % 101 = 0 GROUP BY (payload)) AS q2
WHERE q1.id = q2.id;') AS str
WHERE str NOT LIKE '%Workers%';

RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET max_parallel_workers;
RESET max_parallel_workers_per_gather;
DROP TABLE t;
DROP FUNCTION expln;
DROP EXTENSION aqo;
