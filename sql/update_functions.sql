-- Preliminaries
CREATE EXTENSION IF NOT EXISTS aqo;
SELECT true AS success FROM aqo_reset();

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

SET aqo.mode='intelligent';

SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a;
SELECT count(*) FROM aqo_test1 a, aqo_test2 b, aqo_test2 c WHERE a.a = b.a AND b.a = c.a;

SET aqo.mode='learn';

SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a;
SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a;
SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a;

SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 10;

SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 10 and b.a > 200;
SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 11 and b.a > 200;
SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 12 and b.a > 200;
SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 14 and b.a > 200;

SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 15 and
b.a > 300 and b.a < 500;
SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 15 and
b.a > 300 and b.a < 500;
SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 15 and
b.a > 300 and b.a < 500;

SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 15 and
(b.a > 300 and b.a < 500 or b.a > 100 and b.a < 200);
SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 15 and
(b.a > 300 and b.a < 500 or b.a > 100 and b.a < 200);
SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 15 and
(b.a > 300 and b.a < 500 or b.a > 100 and b.a < 200);
SELECT count(*) FROM aqo_test1 a, aqo_test2 b WHERE a.a=b.a and a.a > 15 and
(b.a > 300 and b.a < 500 or b.a > 100 and b.a < 200);
SET aqo.mode='controlled';

CREATE TABLE aqo_query_texts_dump AS SELECT * FROM aqo_query_texts;
CREATE TABLE aqo_queries_dump AS SELECT * FROM aqo_queries;
CREATE TABLE aqo_query_stat_dump AS SELECT * FROM aqo_query_stat;
CREATE TABLE aqo_data_dump AS SELECT * FROM aqo_data;

SELECT true AS success FROM aqo_reset();

--
-- aqo_query_texts_update() testing.
--

-- Populate aqo_query_texts with dump data.
SELECT aqo_query_texts_update(queryid, query_text) AS res
FROM aqo_query_texts_dump
ORDER BY res;

-- Check if data is the same as in source, no result rows expected.
(TABLE aqo_query_texts EXCEPT TABLE aqo_query_texts_dump)
UNION ALL
(TABLE aqo_query_texts_dump EXCEPT TABLE aqo_query_texts);

-- Update aqo_query_texts with dump data.
SELECT aqo_query_texts_update(queryid, query_text) AS res
FROM aqo_query_texts_dump
ORDER BY res;

-- Check if data is the same as in source, no result rows expected.
(TABLE aqo_query_texts EXCEPT TABLE aqo_query_texts_dump)
UNION ALL
(TABLE aqo_query_texts_dump EXCEPT TABLE aqo_query_texts);

--
-- aqo_queries_update testing.
--

-- Populate aqo_queries with dump data.
SELECT aqo_queries_update(queryid, fs, learn_aqo, use_aqo, auto_tuning) AS res
FROM aqo_queries_dump
ORDER BY res;

-- Check if data is the same as in source, no result rows expected.
(TABLE aqo_queries_dump EXCEPT TABLE aqo_queries)
UNION ALL
(TABLE aqo_queries EXCEPT TABLE aqo_queries_dump);

-- Update aqo_queries with dump data.
SELECT aqo_queries_update(queryid, fs, learn_aqo, use_aqo, auto_tuning) AS res
FROM aqo_queries_dump
ORDER BY res;

-- Check if data is the same as in source, no result rows expected.
(TABLE aqo_queries_dump EXCEPT TABLE aqo_queries)
UNION ALL
(TABLE aqo_queries EXCEPT TABLE aqo_queries_dump);

--
-- aqo_query_stat_update() testing.
--

-- Populate aqo_query_stat with dump data.
SELECT aqo_query_stat_update(queryid, execution_time_with_aqo,
execution_time_without_aqo, planning_time_with_aqo, planning_time_without_aqo,
cardinality_error_with_aqo, cardinality_error_without_aqo, executions_with_aqo,
executions_without_aqo) AS res
FROM aqo_query_stat_dump
ORDER BY res;

-- Check if data is the same as in source, no result rows expected.
(TABLE aqo_query_stat_dump EXCEPT TABLE aqo_query_stat)
UNION ALL
(TABLE aqo_query_stat EXCEPT TABLE aqo_query_stat_dump);

-- Update aqo_query_stat with dump data.
SELECT aqo_query_stat_update(queryid, execution_time_with_aqo,
execution_time_without_aqo, planning_time_with_aqo, planning_time_without_aqo,
cardinality_error_with_aqo, cardinality_error_without_aqo, executions_with_aqo,
executions_without_aqo) AS res
FROM aqo_query_stat_dump
ORDER BY res;

-- Check if data is the same as in source, no result rows expected.
(TABLE aqo_query_stat_dump EXCEPT TABLE aqo_query_stat)
UNION ALL
(TABLE aqo_query_stat EXCEPT TABLE aqo_query_stat_dump);

--
-- aqo_data_update() testing.
--

-- Populate aqo_data with dump data.
SELECT count(*) AS res1 FROM
  aqo_data_dump,
  LATERAL aqo_data_update(fs, fss, nfeatures, features, targets, reliability, oids) AS ret
WHERE ret \gset

-- Check if data is the same as in source, no result rows expected.
(TABLE aqo_data_dump EXCEPT TABLE aqo_data)
UNION ALL
(TABLE aqo_data EXCEPT TABLE aqo_data_dump);

-- Update aqo_data with dump data.
SELECT count(*) AS res2 FROM
  aqo_data_dump,
  LATERAL aqo_data_update(fs, fss, nfeatures, features, targets, reliability, oids) AS ret
WHERE ret \gset

SELECT :res1 = :res2 AS ml_sizes_are_equal;

-- Check if data is the same as in source, no result rows expected.
(TABLE aqo_data_dump EXCEPT TABLE aqo_data)
UNION ALL
(TABLE aqo_data EXCEPT TABLE aqo_data_dump);


-- Reject aqo_query_stat_update if there is NULL elements in array arg.
SELECT aqo_query_stat_update(1, '{NULL, 1}', '{1, 1}', '{1, 1}', '{1, 1}',
'{1, 1}', '{1, 1}', 1, 1);

-- Reject aqo_query_stat_update if arrays don't have the same size.
SELECT aqo_query_stat_update(1, '{1, 1}', '{1, 1, 1}', '{1, 1}', '{1, 1}',
'{1, 1}', '{1, 1}', 1, 1);

-- Reject aqo_query_stat_update if there are negative executions.
SELECT aqo_query_stat_update(1, '{1, 1}', '{1, 1}', '{1, 1}', '{1, 1}',
'{1, 1}', '{1, 1}', -1, 1);
SELECT aqo_query_stat_update(1, '{1, 1}', '{1, 1}', '{1, 1}', '{1, 1}',
'{1, 1}', '{1, 1}', 1, -1);

-- Reject aqo_query_data_update if number of matrix columns and nfeatures
-- are different.
SELECT aqo_data_update(1, 1, 0, '{{1}}', '{1, 1}', '{1, 1}', '{1, 2, 3}');

-- Reject aqo_query_data_update if there is NULL elements in array arg.
SELECT aqo_data_update(1, 1, 1, '{{NULL}}', '{1}', '{1}', '{1, 2, 3}');
SELECT aqo_data_update(1, 1, 1, '{{1}}', '{NULL}', '{1}', '{1, 2, 3}');
SELECT aqo_data_update(1, 1, 1, '{{1}}', '{1}', '{NULL}', '{1, 2, 3}');

-- Reject aqo_query_data_update if Oids is NULL.
SELECT aqo_data_update(1, 1, 1, '{{1}}', '{1}', '{1}', NULL);

-- Reject aqo_query_data_update if arrays don't have the same number of rows.
SELECT aqo_data_update(1, 1, 1, '{{1}}', '{1, 1}', '{1}', '{1, 2, 3}');
SELECT aqo_data_update(1, 1, 1, '{{1}}', '{1}', '{1, 1}', '{1, 2, 3}');
SELECT aqo_data_update(1, 1, 1, '{{1}, {2}}', '{1}', '{1}', '{1, 2, 3}');

SET aqo.mode='disabled';

DROP EXTENSION aqo CASCADE;

DROP TABLE aqo_test1, aqo_test2;
DROP TABLE aqo_query_texts_dump, aqo_queries_dump, aqo_query_stat_dump, aqo_data_dump;
