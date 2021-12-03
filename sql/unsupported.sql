CREATE EXTENSION aqo;
SET aqo.mode = 'learn';
SET aqo.show_details = 'on';

DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT (gs.* / 50) AS x FROM generate_series(1,1000) AS gs;

SELECT str FROM expln('
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
	SELECT * FROM t GROUP BY (x) HAVING x > 3;
') AS str WHERE str NOT LIKE '%Memory Usage%';

-- Do not support having clauses for now.
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
	SELECT * FROM t GROUP BY (x) HAVING x > 3;

DROP EXTENSION aqo;
