-- Testing for working with equivalence classes

CREATE EXTENSION IF NOT EXISTS aqo;
SET aqo.show_details = 'on';
SET aqo.show_hash = 'off';
SET aqo.mode = 'forced';

--
-- Returns string-by-string explain of a query. Made for removing some strings
-- from the explain output.
--
CREATE OR REPLACE FUNCTION expln(query_string text) RETURNS SETOF text AS $$
BEGIN
    RETURN QUERY
        EXECUTE format('EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) %s', query_string);
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

-- Integer fields
CREATE TABLE aqo_test_int(a int, b int, c int);
INSERT INTO aqo_test_int SELECT (x/10)::int, (x/100)::int, (x/1000)::int
FROM generate_series(0, 9999) x;
ANALYZE aqo_test_int;

CREATE TABLE aqo_test_int1(a int, b int, c int);
INSERT INTO aqo_test_int1 SELECT (x/10)::int, (x/10)::int, (x/10)::int
FROM generate_series(0, 999) x;
ANALYZE aqo_test_int1;

SELECT true AS success FROM aqo_reset();

-- Not equivalent queries
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE a = b AND a = 0;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE a = c AND a = 0;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE b = c AND a = 0;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE b = a AND c = b AND a = any('{0, 1, 2}'::int[]);

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE b = a AND c = b AND a = all('{0, 1, 2}'::int[]);
-- Must be 5
SELECT count(*) FROM aqo_data;
SELECT true AS success FROM aqo_reset();

-- Equivalent queries
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE a = b AND a = c AND a = 0;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE a = b AND b = c AND a = 0;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE a = b AND a = c AND b = c AND a = 0;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE a = b AND b = c AND a = 0 AND b = 0;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE a = b AND b = c AND a = 0 AND c = 0;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE a = b AND b = c AND a = 0 AND b = 0 AND c = 0;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_int
WHERE b = a AND c = b AND 0 = a AND 0 = b AND 0 = c;

-- Must be 1
SELECT count(*) FROM aqo_data;
SELECT true AS success FROM aqo_reset();


-- Tests with JOIN clauses.

-- Case 1.
-- 4 cols in 1 eclass, all of them is 0.
-- 3 nodes with unique FSS.
SELECT str AS result FROM expln('
SELECT * FROM (
	SELECT * FROM aqo_test_int
	WHERE a = b AND b = 0) t1
JOIN (
	SELECT * FROM aqo_test_int1
	WHERE a = b) t2
ON t1.a = t2.a') AS str
WHERE str NOT LIKE '%Memory%';

-- Case 2.
-- 4 cols in 2 eclasses, 2 is 0 and 2 is 1.
-- The top node must be unique, but all of nodes like in a query of case 1.
SELECT str AS result FROM expln('
SELECT * FROM (
	SELECT * FROM aqo_test_int
	WHERE a = b AND b = 0) t1
JOIN (
	SELECT * FROM aqo_test_int1
	WHERE a = b) t2
ON 1 = t2.a') AS str
WHERE str NOT LIKE '%Memory%';

-- Case 3.
-- 4 cols in 2 eclasses, 2 is 0 and 2 is equal but not a const.
-- 1 scan node with FSS like in case 2 and 2 nodes with unique FSS.
SELECT str AS result FROM expln('
SELECT * FROM (
	SELECT * FROM aqo_test_int
	WHERE a = b) t1
JOIN (
	SELECT * FROM aqo_test_int1
	WHERE a = b) t2
ON t1.a = 0') AS str
WHERE str NOT LIKE '%Memory%';

-- Case 4.
-- 4 cols in 1 eclass, all of them is 0.
-- 3 nodes with unique FSS. This is not case 1, because it is SEMI-JOIN.
SELECT str AS result FROM expln('
SELECT * FROM (
	SELECT * FROM aqo_test_int
	WHERE a = b AND b = 0) t1
WHERE EXISTS (
	SELECT * FROM aqo_test_int1
	WHERE a = b AND t1.a = a)') AS str
WHERE str NOT LIKE '%Memory%';

-- Case 5.
-- 4 cols in 1 eclass, all of them is 0.
-- The top node with unique FSS. Leaf nodes like in the case 4.
SELECT str AS result FROM expln('
SELECT * FROM (
	SELECT * FROM aqo_test_int
	WHERE a = b AND b = 0) t1
WHERE NOT EXISTS (
	SELECT * FROM aqo_test_int1
	WHERE a = b AND t1.a = a)') AS str
WHERE str NOT LIKE '%Memory%';

-- Must be 10 rows.
SELECT count(*) FROM aqo_data;
SELECT true AS success FROM aqo_reset();

-- Case 6.
-- 4 cols in 1 eclass.
SELECT str AS result FROM expln('
SELECT * FROM (
	SELECT * FROM aqo_test_int
	WHERE b = a) t1
JOIN (
	SELECT * FROM aqo_test_int1
	WHERE b = a) t2
ON t1.a = t2.b') AS str
WHERE str NOT LIKE '%Memory%';

SELECT str AS result FROM expln('
SELECT * FROM (
	SELECT * FROM aqo_test_int
	WHERE b = a) t1
JOIN (
	SELECT * FROM aqo_test_int1
	WHERE b = a) t2
ON t1.a = t2.a') AS str
WHERE str NOT LIKE '%Memory%';

SELECT str AS result FROM expln('
SELECT * FROM (
	SELECT * FROM aqo_test_int
	WHERE a = b) t1
JOIN (
	SELECT * FROM aqo_test_int1
	WHERE a = b) t2
ON t1.a = t2.a') AS str
WHERE str NOT LIKE '%Memory%';

SELECT str AS result FROM expln('
SELECT * FROM (
	SELECT * FROM aqo_test_int
	WHERE a = b) t1
JOIN (
	SELECT * FROM aqo_test_int1
	WHERE a = b) t2
ON t1.b = t2.b') AS str
WHERE str NOT LIKE '%Memory%';

SELECT str AS result FROM expln('
SELECT * FROM (
	SELECT * FROM aqo_test_int
	WHERE a = b) t1
JOIN (
	SELECT * FROM aqo_test_int1
	WHERE a = b) t2
ON t1.b::text = t2.b::text') AS str
WHERE str NOT LIKE '%Memory%';

-- Must be 4 rows.
SELECT count(*) FROM aqo_data;
SELECT true AS success FROM aqo_reset();


-- Text fields
CREATE TABLE aqo_test_text(a text, b text, c text);
INSERT INTO aqo_test_text
SELECT (x/10)::text, (x/100)::text, (x/1000)::text
FROM generate_series(0, 9999) x;
ANALYZE aqo_test_text;

SELECT true AS success FROM aqo_reset();
-- Not equivalent queries
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_text
WHERE a = b AND a = '0';

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_text
WHERE a = c AND a = '0';

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_text
WHERE b = c AND a = '0';
-- Must be 3
SELECT count(*) FROM aqo_data;
SELECT true AS success FROM aqo_reset();

-- Equivalent queries
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_text
WHERE a = b AND a = c AND a = '0';

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_text
WHERE a = b AND b = c AND a = '0';

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_text
WHERE a = b AND a = c AND b = c AND a = '0';

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_text
WHERE a = b AND b = c AND a = '0' AND b = '0';

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_text
WHERE a = b AND b = c AND a = '0' AND c = '0';

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_text
WHERE a = b AND b = c AND a = '0' AND b = '0' AND c = '0';
-- Must be 1
SELECT count(*) FROM aqo_data;
SELECT true AS success FROM aqo_reset();


-- JSONB fields
CREATE TABLE aqo_test_jsonb(a jsonb, b jsonb, c jsonb);
INSERT INTO aqo_test_jsonb SELECT
to_jsonb(x/10), to_jsonb(x/100), to_jsonb(x/1000)
FROM generate_series(0, 9999) x;
ANALYZE aqo_test_jsonb;

SELECT true AS success FROM aqo_reset();
-- Not equivalent queries
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_jsonb
WHERE a = b AND a = '0'::jsonb;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_jsonb
WHERE a = c AND a = '0'::jsonb;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_jsonb
WHERE b = c AND a = '0'::jsonb;
-- Must be 3
SELECT count(*) FROM aqo_data;
SELECT true AS success FROM aqo_reset();

-- Equivalent queries
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_jsonb
WHERE a = b AND a = c AND a = '0'::jsonb;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_jsonb
WHERE a = b AND b = c AND a = '0'::jsonb;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_jsonb
WHERE a = b AND a = c AND b = c AND a = '0'::jsonb;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_jsonb
WHERE a = b AND b = c AND a = '0'::jsonb AND b = '0'::jsonb;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_jsonb
WHERE a = b AND b = c AND a = '0'::jsonb AND c = '0'::jsonb;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_jsonb
WHERE a = b AND b = c AND a = '0'::jsonb AND b = '0'::jsonb AND c = '0'::jsonb;
-- Must be 1
SELECT count(*) FROM aqo_data;
SELECT true AS success FROM aqo_reset();


-- BOX fields
CREATE TABLE aqo_test_box(a box, b box, c box);
INSERT INTO aqo_test_box SELECT
('((0,0), ('||(x/10)||', '||(x/10)||'))')::box,
('((0,0), ('||(x/100)||', '||(x/100)||'))')::box,
('((0,0), ('||(x/1000)||', '||(x/1000)||'))')::box
FROM generate_series(0, 9999) x;
ANALYZE aqo_test_box;

SELECT true AS success FROM aqo_reset();
-- Not equivalent queries
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a = b AND a = c AND a = '((0,0), (0,0))'::box;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a = b AND b = c AND a = '((0,0), (0,0))'::box;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a = b AND a = c AND b = c AND a = '((0,0), (0,0))'::box;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a = b AND b = c AND a = '((0,0), (0,0))'::box AND b = '((0,0), (0,0))'::box;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a = b AND b = c AND a = '((0,0), (0,0))'::box AND c = '((0,0), (0,0))'::box;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a = b AND b = c AND a = '((0,0), (0,0))'::box AND b = '((0,0), (0,0))'::box AND c = '((0,0), (0,0))'::box;
-- Must be 6
SELECT count(*) FROM aqo_data;
SELECT true AS success FROM aqo_reset();

-- Not equivalent queries too
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a ~= b AND a ~= c AND a ~= '((0,0), (0,0))'::box;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a ~= b AND b ~= c AND a ~= '((0,0), (0,0))'::box;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a ~= b AND a ~= c AND b ~= c AND a ~= '((0,0), (0,0))'::box;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a ~= b AND b ~= c AND a ~= '((0,0), (0,0))'::box AND b ~= '((0,0), (0,0))'::box;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a ~= b AND b ~= c AND a ~= '((0,0), (0,0))'::box AND c ~= '((0,0), (0,0))'::box;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM aqo_test_box
WHERE a ~= b AND b ~= c AND a ~= '((0,0), (0,0))'::box AND b ~= '((0,0), (0,0))'::box AND c ~= '((0,0), (0,0))'::box;
-- Must be 6
SELECT count(*) FROM aqo_data;
SELECT true AS success FROM aqo_reset();

DROP TABLE aqo_test_int;
DROP TABLE aqo_test_text;
DROP TABLE aqo_test_jsonb;
DROP TABLE aqo_test_box;

DROP EXTENSION aqo;
