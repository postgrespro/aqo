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
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

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
DROP TABLE tmp1;

SET aqo.mode = 'controlled';

UPDATE aqo_queries SET learn_aqo = false, use_aqo = false, auto_tuning = false;

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

UPDATE aqo_queries SET learn_aqo = false, use_aqo = true, auto_tuning = false;

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

EXPLAIN verbose SELECT t1.a AS a, t2.a AS b, t3.a AS c, t4.a AS d
FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3, aqo_test1 AS t4
WHERE t1.a = t2.b AND t2.a = t3.b AND t3.a = t4.b;

DROP INDEX aqo_test0_idx_a;
DROP TABLE aqo_test0;

DROP INDEX aqo_test1_idx_a;
DROP TABLE aqo_test1;

DROP EXTENSION aqo;
