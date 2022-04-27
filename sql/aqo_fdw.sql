-- Tests on cardinality estimation of FDW-queries:
-- simple ForeignScan.
-- JOIN push-down (check push of baserestrictinfo and joininfo)
-- Aggregate push-down
-- Push-down of groupings with HAVING clause.

-- Switch off parallel workers because of unsteadiness.
-- Do this in each aqo test separately, so that server regression tests pass
-- with aqo's temporary configuration file loaded.
SET max_parallel_workers TO 0;

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

CREATE USER MAPPING FOR PUBLIC SERVER loopback;

CREATE TABLE local (x int);
CREATE FOREIGN TABLE frgn(x int) SERVER loopback OPTIONS (table_name 'local');
INSERT INTO frgn (x) VALUES (1);
ANALYZE local;

-- Utility tool. Allow to filter system-dependent strings from explain output.
CREATE FUNCTION expln(query_string text) RETURNS SETOF text AS $$
BEGIN
    RETURN QUERY
        EXECUTE format('%s', query_string);
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

-- Trivial foreign scan.
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT x FROM frgn;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT x FROM frgn;

-- Push down base filters. Use verbose mode to see filters.
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE))
SELECT x FROM frgn WHERE x < 10;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE)
SELECT x FROM frgn WHERE x < 10;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT x FROM frgn WHERE x < -10; -- AQO ignores constants

-- Trivial JOIN push-down.
SELECT str FROM expln('
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM frgn AS a, frgn AS b WHERE a.x=b.x;
') AS str WHERE str NOT LIKE '%Sort Method%';
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE)
SELECT * FROM frgn AS a, frgn AS b WHERE a.x=b.x;

-- TODO: Non-mergejoinable join condition.
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM frgn AS a, frgn AS b WHERE a.x<b.x;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE)
SELECT * FROM frgn AS a, frgn AS b WHERE a.x<b.x;

DROP EXTENSION aqo CASCADE;
DROP EXTENSION postgres_fdw CASCADE;
DROP TABLE local;

