CREATE EXTENSION aqo;
SET aqo.join_threshold = 0;
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
-- Doesn't support GROUPING SETS clause
--
SELECT count(*) FROM (SELECT x, y FROM t1 GROUP BY GROUPING SETS ((x,y), (x), (y), ())) AS q1;
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM (SELECT x, y FROM t1 GROUP BY GROUPING SETS ((x,y), (x), (y), ())) AS q1;

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
SELECT str FROM expln('
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT count(*) FROM
	(SELECT * FROM t WHERE x % 3 < (SELECT avg(x) FROM t t0 WHERE t0.x = t.x)) AS q1
		JOIN
	(SELECT * FROM t WHERE x % 3 < (SELECT avg(x) FROM t t0 WHERE t0.x <> t.x)) AS q2
		ON q1.x = q2.x+1;
') AS str WHERE str NOT LIKE '%Memory Usage%';

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

-- AQO needs to predict total fetched tuples in a table.
--
-- At a non-leaf node we have prediction about input tuples - is a number of
-- predicted output rows in underlying node. But for Scan nodes we don't have
-- any prediction on number of fetched tuples.
-- So, if selectivity was wrong we could make bad choice of Scan operation.
-- For example, we could choose suboptimal index.

-- Turn off statistics gathering for simple demonstration of filtering problem.
ALTER TABLE t SET (autovacuum_enabled = 'false');
CREATE INDEX ind1 ON t(x);

SELECT count(*) FROM t WHERE x < 3 AND mod(x,3) = 1;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
	SELECT count(*) FROM t WHERE x < 3 AND mod(x,3) = 1;

-- Because of bad statistics we use a last created index instead of best choice.
-- Here we filter more tuples than with the ind1 index.
CREATE INDEX ind2 ON t(mod(x,3));
SELECT count(*) FROM t WHERE x < 3 AND mod(x,3) = 1;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
	SELECT count(*) FROM t WHERE x < 3 AND mod(x,3) = 1;

-- Best choice is ...
ANALYZE t;
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM t WHERE x < 3 AND mod(x,3) = 1;

-- XXX: Do we stuck into an unstable behavior of an error value?
-- Live with this variant of the test for some time.
SELECT to_char(error, '9.99EEEE')::text AS error, query_text
FROM aqo_cardinality_error(true) cef, aqo_query_texts aqt
WHERE aqt.queryid = cef.id
ORDER BY (md5(query_text),error) DESC;

DROP TABLE t,t1 CASCADE; -- delete all tables used in the test

SELECT count(*) FROM aqo_data; -- Just to detect some changes in the logic. May some false positives really bother us here?
SELECT * FROM aqo_cleanup();
SELECT count(*) FROM aqo_data; -- No one row should be returned

-- Look for any remaining queries in the ML storage.
SELECT to_char(error, '9.99EEEE')::text AS error, query_text
FROM aqo_cardinality_error(true) cef, aqo_query_texts aqt
WHERE aqt.queryid = cef.id
ORDER BY (md5(query_text),error) DESC;

SELECT 1 FROM aqo_reset();
DROP EXTENSION aqo;
