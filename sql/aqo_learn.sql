-- The function just copied from stats_ext.sql
create function check_estimated_rows(text) returns table (estimated int, actual int)
language plpgsql as
$$
declare
    ln text;
    tmp text[];
    first_row bool := true;
begin
    for ln in
        execute format('explain analyze %s', $1)
    loop
        if first_row then
            first_row := false;
            tmp := regexp_match(ln, 'rows=(\d*) .* rows=(\d*)');
            return query select tmp[1]::int, tmp[2]::int;
        end if;
    end loop;
end;
$$;

CREATE TABLE aqo_test0(a int, b int, c int, d int);
WITH RECURSIVE t(a, b, c, d)
AS (
   VALUES (0, 0, 0, 0)
   UNION ALL
   SELECT t.a + 1, t.b + 1, t.c + 1, t.d + 1 FROM t WHERE t.a < 2000
) INSERT INTO aqo_test0 (SELECT * FROM t);
CREATE INDEX aqo_test0_idx_a ON aqo_test0 (a);
ANALYZE aqo_test0;

CREATE TABLE aqo_test1(a int, b int);
WITH RECURSIVE t(a, b)
AS (
   VALUES (1, 2)
   UNION ALL
   SELECT t.a + 1, t.b + 1 FROM t WHERE t.a < 20
) INSERT INTO aqo_test1 (SELECT * FROM t);
CREATE INDEX aqo_test1_idx_a ON aqo_test1 (a);
ANALYZE aqo_test1;

CREATE EXTENSION aqo;
SET aqo.join_threshold = 0;

SET aqo.mode = 'intelligent';

EXPLAIN SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;

EXPLAIN SELECT * FROM aqo_test0
WHERE a < 4 AND b < 4 AND c < 4 AND d < 4;

EXPLAIN SELECT * FROM aqo_test0
WHERE a < 5 AND b < 5 AND c < 5 AND d < 5;

EXPLAIN SELECT t1.a, t2.b FROM aqo_test0 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t1.b < 1 AND t2.c < 1 AND t2.d < 1;

EXPLAIN SELECT t1.a, t2.b FROM aqo_test1 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t2.b < 1 AND t2.c < 1 AND t2.d < 1 AND t1.a = t2.a;

EXPlAIN SELECT t1.a, t2.b, t3.c
FROM aqo_test1 AS t1, aqo_test0 AS t2, aqo_test0 AS t3
WHERE t1.a < 1 AND t3.b < 1 AND t2.c < 1 AND t3.d < 0 AND t1.a = t2.a AND t1.b = t3.b;

EXPLAIN SELECT t1.a AS a, t2.a AS b, t3.a AS c
FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3
WHERE t1.a = t2.b AND t2.a = t3.b;

EXPLAIN SELECT t1.a AS a, t2.a AS b, t3.a AS c, t4.a AS d
FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3, aqo_test1 AS t4
WHERE t1.a = t2.b AND t2.a = t3.b AND t3.a = t4.b;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 4 AND b < 4 AND c < 4 AND d < 4;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 4 AND b < 4 AND c < 4 AND d < 4;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 5 AND b < 5 AND c < 5 AND d < 5;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT t1.a, t2.b FROM aqo_test0 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t1.b < 1 AND t2.c < 1 AND t2.d < 1;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT t1.a, t2.b FROM aqo_test1 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t2.b < 1 AND t2.c < 1 AND t2.d < 1 AND t1.a = t2.a;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT t1.a, t2.b, t3.c
FROM aqo_test1 AS t1, aqo_test0 AS t2, aqo_test0 AS t3
WHERE t1.a < 1 AND t3.b < 1 AND t2.c < 1 AND t3.d < 0 AND t1.a = t2.a AND t1.b = t3.b;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT t1.a AS a, t2.a AS b, t3.a AS c
FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3
WHERE t1.a = t2.b AND t2.a = t3.b;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT t1.a AS a, t2.a AS b, t3.a AS c, t4.a AS d
FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3, aqo_test1 AS t4
WHERE t1.a = t2.b AND t2.a = t3.b AND t3.a = t4.b;
SELECT count(*) FROM tmp1;

-- Remove data on some unneeded instances of tmp1 table.
SELECT * FROM aqo_cleanup();

-- Result of the query below should be empty
SELECT * FROM aqo_query_texts aqt1, aqo_query_texts aqt2
WHERE aqt1.query_text = aqt2.query_text AND aqt1.queryid <> aqt2.queryid;

-- Fix the state of the AQO data
SELECT min(reliability),sum(nfeatures),query_text
FROM aqo_data ad, aqo_query_texts aqt
WHERE aqt.queryid = ad.fs
GROUP BY (query_text) ORDER BY (md5(query_text))
;

DROP TABLE tmp1;

SET aqo.mode = 'controlled';

SELECT count(*) FROM
	(SELECT queryid AS id FROM aqo_queries) AS q1,
	LATERAL aqo_queries_update(q1.id, NULL, false, false, false)
; -- Disable all AQO query classes

EXPLAIN SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;

EXPLAIN SELECT * FROM aqo_test0
WHERE a < 4 AND b < 4 AND c < 4 AND d < 4;

EXPLAIN SELECT * FROM aqo_test0
WHERE a < 5 AND b < 5 AND c < 5 AND d < 5;

EXPLAIN SELECT t1.a, t2.b FROM aqo_test0 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t1.b < 1 AND t2.c < 1 AND t2.d < 1;

EXPLAIN SELECT t1.a, t2.b FROM aqo_test1 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t2.b < 1 AND t2.c < 1 AND t2.d < 1 AND t1.a = t2.a;

EXPLAIN SELECT t1.a, t2.b, t3.c
FROM aqo_test1 AS t1, aqo_test0 AS t2, aqo_test0 AS t3
WHERE t1.a < 1 AND t3.b < 1 AND t2.c < 1 AND t3.d < 0 AND t1.a = t2.a AND t1.b = t3.b;

SELECT * FROM check_estimated_rows('
   SELECT t1.a AS a, t2.a AS b, t3.a AS c
   FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3
   WHERE t1.a = t2.b AND t2.a = t3.b;
');

EXPLAIN SELECT t1.a AS a, t2.a AS b, t3.a AS c, t4.a AS d
FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3, aqo_test1 AS t4
WHERE t1.a = t2.b AND t2.a = t3.b AND t3.a = t4.b;

SELECT count(*) FROM
	(SELECT queryid AS id FROM aqo_queries) AS q1,
	LATERAL aqo_queries_update(q1.id, NULL, false, true, false)
; -- learn = false, use = true, tuning = false

EXPLAIN SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;

EXPLAIN SELECT * FROM aqo_test0
WHERE a < 4 AND b < 4 AND c < 4 AND d < 4;

EXPLAIN SELECT * FROM aqo_test0
WHERE a < 5 AND b < 5 AND c < 5 AND d < 5;

EXPLAIN SELECT t1.a, t2.b FROM aqo_test0 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t1.b < 1 AND t2.c < 1 AND t2.d < 1;

EXPLAIN SELECT t1.a, t2.b FROM aqo_test1 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t2.b < 1 AND t2.c < 1 AND t2.d < 1 AND t1.a = t2.a;

EXPLAIN SELECT t1.a, t2.b, t3.c
FROM aqo_test1 AS t1, aqo_test0 AS t2, aqo_test0 AS t3
WHERE t1.a < 1 AND t3.b < 1 AND t2.c < 1 AND t3.d < 0 AND t1.a = t2.a AND t1.b = t3.b;

SELECT * FROM check_estimated_rows('
   SELECT t1.a AS a, t2.a AS b, t3.a AS c
   FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3
   WHERE t1.a = t2.b AND t2.a = t3.b;
');

SELECT * FROM check_estimated_rows('
   SELECT t1.a AS a, t2.a AS b, t3.a AS c, t4.a AS d
   FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3, aqo_test1 AS t4
   WHERE t1.a = t2.b AND t2.a = t3.b AND t3.a = t4.b;
');

-- Test limit on number of joins
SET aqo.mode = 'learn';

SELECT * FROM aqo_drop_class(0);
SELECT * FROM aqo_drop_class(42);

-- Remove all data from ML knowledge base
SELECT count(*) FROM (
SELECT aqo_drop_class(q1.id::bigint) FROM (
    SELECT queryid AS id
    FROM aqo_queries WHERE queryid <> 0) AS q1
) AS q2;
SELECT count(*) FROM aqo_data;

SET aqo.join_threshold = 3;
SELECT * FROM check_estimated_rows('SELECT * FROM aqo_test1;');
SELECT * FROM check_estimated_rows('
  SELECT * FROM aqo_test1 AS t1, aqo_test1 AS t2 WHERE t1.a = t2.b');
SELECT count(*) FROM aqo_data; -- Return 0 - do not learn on the queries above

SELECT * FROM check_estimated_rows('
   SELECT *
   FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3, aqo_test1 AS t4
   WHERE t1.a = t2.b AND t2.a = t3.b AND t3.a = t4.b;
'); -- Learn on the query
SELECT count(*) FROM
  (SELECT fs FROM aqo_data GROUP BY (fs)) AS q1
;
SELECT query_text FROM aqo_query_texts WHERE queryid <> 0; -- Check query

SET aqo.join_threshold = 1;
SELECT * FROM check_estimated_rows('SELECT * FROM aqo_test1;');
SELECT * FROM check_estimated_rows(
  'SELECT * FROM aqo_test1 AS t1, aqo_test1 AS t2 WHERE t1.a = t2.b');
SELECT count(*) FROM
  (SELECT fs FROM aqo_data GROUP BY (fs)) AS q1
; -- Learn on a query with one join

SET aqo.join_threshold = 0;
SELECT * FROM check_estimated_rows('SELECT * FROM aqo_test1;');
SELECT count(*) FROM
  (SELECT fs FROM aqo_data GROUP BY (fs)) AS q1
; -- Learn on the query without any joins now

SET aqo.join_threshold = 1;
SELECT * FROM check_estimated_rows('SELECT * FROM aqo_test1 t1 JOIN aqo_test1 AS t2 USING (a)');
SELECT count(*) FROM
  (SELECT fs FROM aqo_data GROUP BY (fs)) AS q1
; -- See one more query in the AQO knowledge base

SELECT * FROM check_estimated_rows('WITH selected AS (SELECT * FROM aqo_test1 t1) SELECT count(*) FROM selected');
SELECT * FROM check_estimated_rows('
  WITH selected AS (
    SELECT * FROM aqo_test1 t1 JOIN aqo_test1 AS t2 USING (a)
  ) SELECT count(*) FROM selected')
;
SELECT count(*) FROM (SELECT fs FROM aqo_data GROUP BY (fs)) AS q1; -- +1

-- InitPlan
SELECT * FROM check_estimated_rows('
  SELECT * FROM aqo_test1 AS t1 WHERE t1.a IN (
    SELECT t2.a FROM aqo_test1 AS t2 JOIN aqo_test1 AS t3 ON (t2.b = t3.a)
  )');
SELECT count(*) FROM (SELECT fs FROM aqo_data GROUP BY (fs)) AS q1; -- +1

-- SubPlan
SELECT * FROM check_estimated_rows('
  SELECT (
    SELECT avg(t2.a) FROM aqo_test1 AS t2 JOIN aqo_test1 AS t3 ON (t2.b = t3.a) AND (t2.a = t1.a)
  ) FROM aqo_test1 AS t1;
');
SELECT count(*) FROM (SELECT fs FROM aqo_data GROUP BY (fs)) AS q1; -- +1

-- Subquery
SET aqo.join_threshold = 3;
SELECT * FROM check_estimated_rows('
  SELECT * FROM aqo_test1 AS t1,
    (SELECT t2.a FROM aqo_test1 AS t2 JOIN aqo_test1 AS t3 ON (t2.b = t3.a)) q1
  WHERE q1.a*t1.a = t1.a + 15;
'); -- Two JOINs, ignore it
SELECT count(*) FROM (SELECT fs FROM aqo_data GROUP BY (fs)) AS q1; -- +1
SET aqo.join_threshold = 2;
SELECT * FROM check_estimated_rows('
  SELECT * FROM aqo_test1 AS t1,
    (SELECT t2.a FROM aqo_test1 AS t2 JOIN aqo_test1 AS t3 ON (t2.b = t3.a)) q1
  WHERE q1.a*t1.a = t1.a + 15;
'); -- One JOIN from subquery, another one from the query
SELECT count(*) FROM (SELECT fs FROM aqo_data GROUP BY (fs)) AS q1; -- +1

SELECT * FROM check_estimated_rows('
  WITH selected AS (
    SELECT t2.a FROM aqo_test1 t1 JOIN aqo_test1 AS t2 USING (a)
  ) SELECT count(*) FROM aqo_test1 t3, selected WHERE selected.a = t3.a')
; -- One JOIN extracted from CTE, another - from a FROM part of the query
SELECT count(*) FROM (SELECT fs FROM aqo_data GROUP BY (fs)) AS q1; -- +1

DROP FUNCTION check_estimated_rows;
RESET aqo.join_threshold;
DROP INDEX aqo_test0_idx_a;
DROP TABLE aqo_test0;
DROP INDEX aqo_test1_idx_a;
DROP TABLE aqo_test1;

-- XXX: extension dropping doesn't clear file storage. Do it manually.
SELECT 1 FROM aqo_reset();

DROP EXTENSION aqo;
