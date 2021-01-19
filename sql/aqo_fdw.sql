-- Tests on cardinality estimation of FDW-queries:
-- simple ForeignScan.
-- JOIN push-down (check push of baserestrictinfo and joininfo)
-- Aggregate push-down
-- Push-down of groupings with HAVING clause.

CREATE EXTENSION aqo;
CREATE EXTENSION postgres_fdw;
SET aqo.mode = 'learn';
SET aqo.details = 'true'; -- show AQO info for each node and entire query.
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

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT x FROM frgn;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT x FROM frgn;

-- Push down base filters.
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE) SELECT x FROM frgn WHERE x < 10;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE) SELECT x FROM frgn WHERE x < 10;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT x FROM frgn WHERE x < -10; -- AQO ignores constants

DROP EXTENSION aqo;

