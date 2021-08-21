CREATE EXTENSION aqo;
SET aqo.mode = 'disabled';
SET aqo.force_collect_stat = 'on';

--
-- num of generate_series(1,1000000) query should be the first
--
SELECT count(*) FROM generate_series(1,1000000);
SELECT num FROM top_time_queries(10) AS tt WHERE
    tt.fspace_hash = (SELECT fspace_hash FROM aqo_queries WHERE
        aqo_queries.query_hash = (SELECT aqo_query_texts.query_hash FROM aqo_query_texts
            WHERE query_text = 'SELECT count(*) FROM generate_series(1,1000000);'));

--
-- num of query uses table t2 should be bigger than num of query uses table t1 and be the fisrt
--
CREATE TABLE t1 AS SELECT mod(gs,10) AS x, mod(gs+1,10) AS y
	FROM generate_series(1,1000) AS gs;
CREATE TABLE t2 AS SELECT mod(gs,10) AS x, mod(gs+1,10) AS y
	FROM generate_series(1,100000) AS gs;
SELECT count(*) FROM (SELECT x, y FROM t1 GROUP BY GROUPING SETS ((x,y), (x), (y), ())) AS q1;
SELECT count(*) FROM (SELECT x, y FROM t2 GROUP BY GROUPING SETS ((x,y), (x), (y), ())) AS q1;

SELECT num FROM top_error_queries(10) AS te WHERE
    te.fspace_hash = (SELECT fspace_hash FROM aqo_queries WHERE
        aqo_queries.query_hash = (SELECT aqo_query_texts.query_hash FROM aqo_query_texts
            WHERE query_text = 'SELECT count(*) FROM (SELECT x, y FROM t2 GROUP BY GROUPING SETS ((x,y), (x), (y), ())) AS q1;'));