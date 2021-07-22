CREATE EXTENSION aqo;
SET aqo.mode = 'learn';
SET aqo.show_details = 'on';

DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT (gs.* / 50) AS x FROM generate_series(1,1000) AS gs;
ANALYZE t;

CREATE TABLE t1 AS SELECT mod(gs,10) AS x, mod(gs+1,10) AS y
	FROM generate_series(1,1000) AS gs;
ANALYZE t, t1;

--
-- Do not support HAVING clause for now.
--
SELECT count(*) FROM (SELECT * FROM t GROUP BY (x) HAVING x > 3) AS q1;
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM (SELECT * FROM t GROUP BY (x) HAVING x > 3) AS q1;

--
-- Doesn't estimates GROUP BY clause
--
SELECT count(*) FROM (SELECT count(*) FROM t1 GROUP BY (x,y)) AS q1;
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM (SELECT count(*) FROM t1 GROUP BY (x,y)) AS q1;

SELECT count(*) FROM (SELECT count(*) FROM t1 GROUP BY (x,x*y)) AS q1;
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM (SELECT count(*) FROM t1 GROUP BY (x,x*y)) AS q1;

SELECT count(*) FROM (
	SELECT count(*) AS x FROM (
		SELECT count(*) FROM t1 GROUP BY (x,y)
	) AS q1
) AS q2
WHERE q2.x > 1;

SELECT count(*) FROM (
	SELECT count(*) AS x FROM (
		SELECT count(*) FROM t1 GROUP BY (x,y)
	) AS q1
) AS q2
WHERE q2.x > 1;

EXPLAIN (COSTS OFF)
SELECT count(*) FROM (
	SELECT count(*) AS x FROM (
		SELECT count(*) FROM t1 GROUP BY (x,y)
	) AS q1
) AS q2
WHERE q2.x > 1;

--
-- The subplans issue
--
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

-- Two identical subplans in a clause list
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
	SELECT count(*) FROM t WHERE
		x = (SELECT avg(x) FROM t t0 WHERE t0.x = t.x + 21) OR
		x IN (SELECT avg(x) FROM t t0 WHERE t0.x = t.x + 21);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
	SELECT count(*) FROM t WHERE
		x = (SELECT avg(x) FROM t t0 WHERE t0.x = t.x + 21) OR
		x IN (SELECT avg(x) FROM t t0 WHERE t0.x = t.x + 21);

-- It's OK to use the knowledge for a query with different constants.
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
	SELECT count(*) FROM t WHERE
		x = (SELECT avg(x) FROM t t0 WHERE t0.x = t.x + 22) OR
		x IN (SELECT avg(x) FROM t t0 WHERE t0.x = t.x + 23);

-- Different SubPlans in the quals of leafs of JOIN.
SELECT count(*) FROM
	(SELECT * FROM t WHERE x % 3 < (SELECT avg(x) FROM t t0 WHERE t0.x = t.x)) AS q1
		JOIN
	(SELECT * FROM t WHERE x % 3 < (SELECT avg(x) FROM t t0 WHERE t0.x <> t.x)) AS q2
		ON q1.x = q2.x+1;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT count(*) FROM
	(SELECT * FROM t WHERE x % 3 < (SELECT avg(x) FROM t t0 WHERE t0.x = t.x)) AS q1
		JOIN
	(SELECT * FROM t WHERE x % 3 < (SELECT avg(x) FROM t t0 WHERE t0.x <> t.x)) AS q2
		ON q1.x = q2.x+1;

-- Two identical subplans in a clause
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
	SELECT count(*) FROM t WHERE (SELECT avg(x) FROM t t0 WHERE t0.x = t.x) =
	(SELECT avg(x) FROM t t0 WHERE t0.x = t.x);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
	SELECT count(*) FROM t WHERE (SELECT avg(x) FROM t t0 WHERE t0.x = t.x) =
	(SELECT avg(x) FROM t t0 WHERE t0.x = t.x);

--
-- Not executed nodes
--
SELECT * FROM
	(SELECT * FROM t WHERE x < 0) AS t0
		JOIN
	(SELECT * FROM t WHERE x > 20) AS t1
		USING(x);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM
	(SELECT * FROM t WHERE x < 0) AS t0
		JOIN
	(SELECT * FROM t WHERE x > 20) AS t1
		USING(x);

DROP EXTENSION aqo;
