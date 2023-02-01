CREATE EXTENSION IF NOT EXISTS aqo;
SELECT true AS success FROM aqo_reset();

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

CREATE TABLE aqo_test2(a int);
WITH RECURSIVE t(a)
AS (
   VALUES (0)
   UNION ALL
   SELECT t.a + 1 FROM t WHERE t.a < 100000
) INSERT INTO aqo_test2 (SELECT * FROM t);
CREATE INDEX aqo_test2_idx_a ON aqo_test2 (a);
ANALYZE aqo_test2;

SET aqo.mode = 'controlled';

EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;

EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 5 AND b < 5 AND c < 5 AND d < 5;

SET aqo.mode = 'intelligent';

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
DROP TABLE tmp1;

SET aqo.mode = 'controlled';

SELECT count(*) FROM
	(SELECT queryid AS id FROM aqo_queries) AS q1,
	LATERAL aqo_queries_update(q1.id, NULL, true, false, false)
; -- learn = true, use = false, tuning = false

EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;

EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 5 AND b < 5 AND c < 5 AND d < 5;

EXPLAIN (COSTS FALSE)
SELECT t1.a, t2.b FROM aqo_test0 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t1.b < 1 AND t2.c < 1 AND t2.d < 1;

EXPLAIN (COSTS FALSE)
SELECT t1.a, t2.b FROM aqo_test1 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t2.b < 1 AND t2.c < 1 AND t2.d < 1 AND t1.a = t2.a;

EXPLAIN (COSTS FALSE)
SELECT t1.a, t2.b, t3.c
FROM aqo_test1 AS t1, aqo_test0 AS t2, aqo_test0 AS t3
WHERE t1.a < 1 AND t3.b < 1 AND t2.c < 1 AND t3.d < 0 AND t1.a = t2.a AND t1.b = t3.b;

EXPLAIN (COSTS FALSE)
SELECT t1.a AS a, t2.a AS b, t3.a AS c
FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3
WHERE t1.a = t2.b AND t2.a = t3.b;

SELECT count(*) FROM
	(SELECT queryid AS id FROM aqo_queries) AS q1,
	LATERAL aqo_queries_update(q1.id, NULL, NULL, true, NULL) AS ret
WHERE NOT ret
; -- set use = true

EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;

EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 5 AND b < 5 AND c < 5 AND d < 5;

EXPLAIN (COSTS FALSE)
SELECT t1.a, t2.b FROM aqo_test0 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t1.b < 1 AND t2.c < 1 AND t2.d < 1;

EXPLAIN (COSTS FALSE)
SELECT t1.a, t2.b FROM aqo_test1 AS t1, aqo_test0 AS t2
WHERE t1.a < 1 AND t2.b < 1 AND t2.c < 1 AND t2.d < 1 AND t1.a = t2.a;

EXPLAIN (COSTS FALSE)
SELECT t1.a, t2.b, t3.c
FROM aqo_test1 AS t1, aqo_test0 AS t2, aqo_test0 AS t3
WHERE t1.a < 1 AND t3.b < 1 AND t2.c < 1 AND t3.d < 0 AND t1.a = t2.a AND t1.b = t3.b;

EXPLAIN (COSTS FALSE)
SELECT t1.a AS a, t2.a AS b, t3.a AS c
FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3
WHERE t1.a = t2.b AND t2.a = t3.b;

EXPLAIN (COSTS FALSE)
SELECT t1.a AS a, t2.a AS b, t3.a AS c
FROM aqo_test2 AS t1, aqo_test1 AS t2, aqo_test1 AS t3
WHERE t1.a = t2.b AND t2.a = t3.b;

DROP INDEX aqo_test0_idx_a;
DROP TABLE aqo_test0;
DROP INDEX aqo_test1_idx_a;
DROP TABLE aqo_test1;
DROP INDEX aqo_test2_idx_a;
DROP TABLE aqo_test2;

DROP EXTENSION aqo;
