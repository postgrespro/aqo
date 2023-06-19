-- Preliminaries
CREATE EXTENSION IF NOT EXISTS aqo;
SELECT true AS success FROM aqo_reset();
SET aqo.show_details = on;

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

SET aqo.mode = 'controlled';

EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;

EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 5 AND b < 5 AND c < 5 AND d < 5;

SET aqo.mode = 'forced';

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 5 AND b < 5 AND c < 5 AND d < 5;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 3 AND b < 3 AND c < 3 AND d < 3;

EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 5 AND b < 5 AND c < 5 AND d < 5;

SET aqo.mode = 'forced_controlled';

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 10 AND b < 10 AND c < 10;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

-- Not predict
EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 10 AND b < 10 AND c < 10;

SET aqo.mode = 'forced';

CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 0 AND b < 0 AND c < 0;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

SET aqo.mode = 'forced_controlled';
CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 10 AND b < 10 AND c < 10;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

-- Predict
EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 10 AND b < 10 AND c < 10;

SET aqo.mode = 'forced_frozen';

-- Not learn
CREATE TABLE tmp1 AS SELECT * FROM aqo_test0
WHERE a < 20 AND b < 20 AND c < 20;
SELECT count(*) FROM tmp1;
DROP TABLE tmp1;

-- Predict approximately 9 (interpolation between 1 and 10)
EXPLAIN (COSTS FALSE)
SELECT * FROM aqo_test0
WHERE a < 20 AND b < 20 AND c < 20;

DROP INDEX aqo_test0_idx_a;
DROP TABLE aqo_test0;
DROP INDEX aqo_test1_idx_a;
DROP TABLE aqo_test1;

RESET aqo.show_details;
DROP EXTENSION aqo;
