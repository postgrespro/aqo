-- Switch off parallel workers because of unsteadiness.
-- Do this in each aqo test separately, so that server regression tests pass
-- with aqo's temporary configuration file loaded.
SET max_parallel_workers TO 0;

CREATE EXTENSION aqo;
SET aqo.mode = 'disabled';
SET aqo.force_collect_stat = 'on';

--
-- Dummy test. CREATE TABLE shouldn't find in the ML storage. But a simple
-- select must be in. Also here we test on gathering a stat on temp and plain
-- relations.
--
CREATE TEMP TABLE ttt AS SELECT count(*) AS cnt FROM generate_series(1,10);
CREATE TABLE ttp AS SELECT count(*) AS cnt FROM generate_series(1,10);
SELECT count(*) AS cnt FROM ttt WHERE cnt % 100 = 0;
SELECT count(*) AS cnt FROM ttp WHERE cnt % 100 = 0;
SELECT num FROM top_time_queries(3);

--
-- num of query uses table t2 should be bigger than num of query uses table t1 and be the first
--
CREATE TABLE t1 AS SELECT mod(gs,10) AS x, mod(gs+1,10) AS y
	FROM generate_series(1,1000) AS gs;
CREATE TABLE t2 AS SELECT mod(gs,10) AS x, mod(gs+1,10) AS y
	FROM generate_series(1,100000) AS gs;
SELECT count(*) FROM (SELECT x, y FROM t1 GROUP BY GROUPING SETS ((x,y), (x), (y), ())) AS q1;
SELECT count(*) FROM (SELECT x, y FROM t2 GROUP BY GROUPING SETS ((x,y), (x), (y), ())) AS q1;

SELECT num, to_char(error, '9.99EEEE') FROM show_cardinality_errors(false) AS te
WHERE te.fshash = (
  SELECT fspace_hash FROM aqo_queries
  WHERE aqo_queries.query_hash = (
    SELECT aqo_query_texts.query_hash FROM aqo_query_texts
    WHERE query_text = 'SELECT count(*) FROM (SELECT x, y FROM t2 GROUP BY GROUPING SETS ((x,y), (x), (y), ())) AS q1;'
  )
);

-- Should return zero
SELECT count(*) FROM show_cardinality_errors(true);
