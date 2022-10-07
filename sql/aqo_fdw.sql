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
SET aqo.join_threshold = 0;

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

-- TODO: Should learn on postgres_fdw nodes
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE)
  SELECT * FROM frgn AS a, frgn AS b WHERE a.x=b.x;

CREATE TABLE local_a(aid int primary key, aval text);
CREATE TABLE local_b(bid int primary key, aid int references local_a(aid), bval text);
INSERT INTO local_a SELECT i, 'val_' || i FROM generate_series(1,100) i;
INSERT INTO local_b SELECT i, mod((i+random()*10)::numeric, 10) + 1, 'val_' || i FROM generate_series(1,1000) i;
ANALYZE local_a, local_b;

CREATE FOREIGN TABLE frgn_a(aid int, aval text) SERVER loopback OPTIONS (table_name 'local_a');
CREATE FOREIGN TABLE frgn_b(bid int, aid int, bval text) SERVER loopback OPTIONS (table_name 'local_b');

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * from frgn_a AS a, frgn_b AS b
WHERE a.aid = b.aid AND b.bval like 'val%';

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * from frgn_a AS a, frgn_b AS b
WHERE a.aid = b.aid AND b.bval like 'val%';

-- TODO: Non-mergejoinable join condition.
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM frgn AS a, frgn AS b WHERE a.x<b.x;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE)
SELECT * FROM frgn AS a, frgn AS b WHERE a.x<b.x;

DROP EXTENSION aqo CASCADE;
DROP EXTENSION postgres_fdw CASCADE;
DROP TABLE local;
DROP TABLE local_b;
DROP TABLE local_a;

