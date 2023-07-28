-- Tests on cross-databases interference.

create extension aqo;
set aqo.join_threshold = 0;
set aqo.show_details = on;
set aqo.mode = learn;
set aqo.use = on;
select * from aqo_reset(NULL);

CREATE DATABASE aqo_crossdb_test;
-- Save current database and port.
SELECT current_database() AS old_db \gset
SELECT oid AS old_dbid FROM pg_database WHERE datname = current_database() \gset
SELECT setting AS old_port FROM pg_settings WHERE name = 'port' \gset

CREATE TABLE a (x1 int, x2 int, x3 int);
INSERT INTO a (x1, x2, x3) SELECT mod(ival,10), mod(ival,10), mod(ival,10) FROM generate_series(1,100) As ival;

CREATE TABLE b (y1 int, y2 int, y3 int);
INSERT INTO b (y1, y2, y3) SELECT mod(ival + 1,10), mod(ival + 1,10), mod(ival + 1,10) FROM generate_series(1,1000) As ival;

--
-- Returns string-by-string explain of a query. Made for removing some strings
-- from the explain output.
--
CREATE OR REPLACE FUNCTION expln(query_string text) RETURNS SETOF text AS $$
BEGIN
    RETURN QUERY
        EXECUTE format('EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF) %s', query_string);
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A LEFT JOIN b ON A.x1 = B.y1 WHERE x1 = 5 AND x2 = 5;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT count(*) FROM aqo_data();
SELECT count(*) FROM aqo_queries();
SELECT count(*) FROM aqo_query_texts();
SELECT count(*) FROM aqo_query_stat();


-- Connect to other DB
\c aqo_crossdb_test - - :old_port
create extension aqo;
set aqo.join_threshold = 0;
set aqo.show_details = on;
set aqo.mode = learn;
set aqo.use = on;

CREATE TABLE a (x1 int, x2 int, x3 int);
INSERT INTO a (x1, x2, x3) SELECT mod(ival,10), mod(ival,10), mod(ival,10) FROM generate_series(1,100) As ival;

CREATE OR REPLACE FUNCTION expln(query_string text) RETURNS SETOF text AS $$
BEGIN
    RETURN QUERY
        EXECUTE format('EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF) %s', query_string);
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

SELECT str AS result
FROM expln('
SELECT * FROM a WHERE x1 > 1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT count(*) FROM aqo_data();
SELECT count(*) FROM aqo_queries();
SELECT count(*) FROM aqo_query_texts();
SELECT count(*) FROM aqo_query_stat();

-- Remove aqo info from other DB.
SELECT aqo_reset(:old_dbid);

-- Reconnect to old DB.
\c :old_db - - :old_port
SELECT count(*) FROM aqo_data();
SELECT count(*) FROM aqo_queries();
SELECT count(*) FROM aqo_query_texts();
SELECT count(*) FROM aqo_query_stat();
SELECT aqo_reset(NULL);

DROP DATABASE aqo_crossdb_test;
DROP EXTENSION aqo;