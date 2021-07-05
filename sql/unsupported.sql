CREATE EXTENSION aqo;
SET aqo.mode = 'learn';
SET aqo.show_details = 'on';

DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT (gs.* / 50) AS x FROM generate_series(1,1000) AS gs;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
	SELECT * FROM t GROUP BY (x) HAVING x > 3;

-- Do not support having clauses for now.
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
	SELECT * FROM t GROUP BY (x) HAVING x > 3;

SELECT count(*) FROM t WHERE x = (SELECT avg(x) FROM t WHERE x = 1);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
	SELECT count(*) FROM t WHERE x = (
		SELECT avg(x) FROM t WHERE x = 1
	);

SELECT count(*) FROM t WHERE x = (SELECT avg(x) FROM t t0 WHERE t0.x = t.x);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
	SELECT count(*) FROM t WHERE x = (
		SELECT avg(x) FROM t t0 WHERE t0.x = t.x
	);
	
DROP EXTENSION aqo;
