-- Tests on cardinality estimation of FDW-queries:
-- simple ForeignScan.
-- JOIN push-down (check push of baserestrictinfo and joininfo)
-- Aggregate push-down
-- Push-down of groupings with HAVING clause.

CREATE EXTENSION aqo;
CREATE EXTENSION postgres_fdw;
SET aqo.mode = 'learn';
SET aqo.show_details = 'true'; -- show AQO info for each node and entire query.
SET aqo.show_hash = 'false'; -- a hash value is system-depended. Ignore it.

DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;
--
-- Returns string-by-string explain of a query. Made for removing some strings
-- from the explain output.
--
CREATE OR REPLACE FUNCTION expln(query_string text default 'select * from table', verbose_p boolean default TRUE) RETURNS SETOF text AS $$
BEGIN
    IF verbose_p=TRUE THEN
        RETURN QUERY EXECUTE format('EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF) %s', query_string);
    else
        RETURN QUERY EXECUTE format('EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) %s', query_string);
    END IF;
    Return;
END;
$$ LANGUAGE PLPGSQL;

CREATE USER MAPPING FOR PUBLIC SERVER loopback;

CREATE TABLE local (x int);
CREATE FOREIGN TABLE frgn(x int) SERVER loopback OPTIONS (table_name 'local');
INSERT INTO frgn (x) VALUES (1);
ANALYZE local;

-- Trivial foreign scan.
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT x FROM frgn;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT x FROM frgn;

-- Push down base filters. Use verbose mode to see filters.
SELECT str AS result
FROM expln('SELECT x FROM frgn WHERE x < 10;', TRUE) AS str
WHERE str NOT LIKE 'Query Identifier%';
SELECT str AS result
FROM expln('SELECT x FROM frgn WHERE x < 10;', TRUE) AS str
WHERE str NOT LIKE 'Query Identifier%';
SELECT str AS result
FROM expln('SELECT x FROM frgn WHERE x < -10;', TRUE) AS str
WHERE str NOT LIKE 'Query Identifier%'; -- AQO ignores constants

-- Trivial JOIN push-down.
SELECT str AS result
FROM expln('SELECT * FROM frgn AS a, frgn AS b WHERE a.x=b.x;', FALSE) AS str
WHERE str NOT LIKE 'Query Identifier%';
SELECT str AS result
FROM expln('SELECT * FROM frgn AS a, frgn AS b WHERE a.x=b.x;', TRUE) AS str
WHERE str NOT LIKE 'Query Identifier%';

-- TODO: Non-mergejoinable join condition.
SELECT str AS result
FROM expln('SELECT * FROM frgn AS a, frgn AS b WHERE a.x<b.x;', FALSE) AS str
WHERE str NOT LIKE 'Query Identifier%';
SELECT str AS result
FROM expln('SELECT * FROM frgn AS a, frgn AS b WHERE a.x<b.x;', TRUE) AS str
WHERE str NOT LIKE 'Query Identifier%';

DROP EXTENSION aqo CASCADE;
DROP EXTENSION postgres_fdw CASCADE;
DROP TABLE local;

