CREATE EXTENSION aqo;
SET aqo.join_threshold = 0;
SET aqo.mode = 'disabled';
SET aqo.force_collect_stat = 'on';

--
-- Dummy test. CREATE TABLE shouldn't be found in the ML storage. But a simple
-- select must recorded. Also here we test on gathering a stat on temp and plain
-- relations.
-- XXX: Right now we ignore queries if no one permanent table is touched.
--
CREATE TEMP TABLE ttt AS SELECT count(*) AS cnt FROM generate_series(1,10);
CREATE TABLE ttp AS SELECT count(*) AS cnt FROM generate_series(1,10);
SELECT count(*) AS cnt FROM ttt WHERE cnt % 100 = 0; -- Ignore it
SELECT count(*) AS cnt FROM ttp WHERE cnt % 100 = 0;
SELECT num FROM aqo_execution_time(true); -- Just for checking, return zero.
SELECT num FROM aqo_execution_time(false);

-- Without the AQO control queries with and without temp tables are logged.
SELECT query_text,nexecs
FROM aqo_execution_time(false) ce, aqo_query_texts aqt
WHERE ce.id = aqt.queryid
ORDER BY (md5(query_text));

--
-- num of query which uses the table t2 should be bigger than num of query which
-- uses the table t1 and must be the first
--
CREATE TABLE t1 AS SELECT mod(gs,10) AS x, mod(gs+1,10) AS y
	FROM generate_series(1,1000) AS gs;
CREATE TABLE t2 AS SELECT mod(gs,10) AS x, mod(gs+1,10) AS y
	FROM generate_series(1,100000) AS gs;
SELECT count(*) FROM (SELECT x, y FROM t1 GROUP BY GROUPING SETS ((x,y), (x), (y), ())) AS q1;
SELECT count(*) FROM (SELECT x, y FROM t2 GROUP BY GROUPING SETS ((x,y), (x), (y), ())) AS q1;

SELECT num, to_char(error, '9.99EEEE') FROM aqo_cardinality_error(false) AS te
WHERE te.fshash = (
  SELECT fs FROM aqo_queries
  WHERE aqo_queries.queryid = (
    SELECT aqo_query_texts.queryid FROM aqo_query_texts
    WHERE query_text = 'SELECT count(*) FROM (SELECT x, y FROM t2 GROUP BY GROUPING SETS ((x,y), (x), (y), ())) AS q1;'
  )
);

-- Should return zero
SELECT count(*) FROM aqo_cardinality_error(true);

-- Fix list of logged queries
SELECT query_text,nexecs
FROM aqo_cardinality_error(false) ce, aqo_query_texts aqt
WHERE ce.id = aqt.queryid
ORDER BY (md5(query_text));

SELECT 1 FROM aqo_reset();
DROP EXTENSION aqo;
