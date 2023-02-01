-- Tests on cardinality estimation of FDW-queries:
-- simple ForeignScan.
-- JOIN push-down (check push of baserestrictinfo and joininfo)
-- Aggregate push-down
-- Push-down of groupings with HAVING clause.

CREATE EXTENSION IF NOT EXISTS aqo;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
SELECT true AS success FROM aqo_reset();

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
SELECT str FROM expln('
  EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE)
    SELECT x FROM frgn WHERE x < 10;
') AS str;
SELECT str FROM expln('
  EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE)
    SELECT x FROM frgn WHERE x < 10;
') AS str;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT x FROM frgn WHERE x < -10; -- AQO ignores constants

-- Trivial JOIN push-down.
SELECT str FROM expln('
  EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
  SELECT * FROM frgn AS a, frgn AS b WHERE a.x=b.x;
') AS str WHERE str NOT LIKE '%Sort Method%';

-- Should learn on postgres_fdw nodes
SELECT str FROM expln('
  EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE)
    SELECT * FROM frgn AS a, frgn AS b WHERE a.x=b.x;
') AS str;

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

-- Partitioned join over foreign tables
set enable_partitionwise_join = on;
ALTER SERVER loopback OPTIONS (ADD fdw_tuple_cost '1.0');

CREATE TABLE local_main_p0(aid int, aval text);
CREATE TABLE local_main_p1(aid int, aval text);
CREATE TABLE main (aid int, aval text) PARTITION BY HASH(aid);

CREATE FOREIGN TABLE main_p0 PARTITION OF main FOR VALUES WITH (MODULUS 3, REMAINDER 0)
	SERVER loopback OPTIONS (table_name 'local_main_p0');
CREATE FOREIGN TABLE main_p1 PARTITION OF main FOR VALUES WITH (MODULUS 3, REMAINDER 1)
	SERVER loopback OPTIONS (table_name 'local_main_p1');
CREATE TABLE main_p2 PARTITION OF main FOR VALUES WITH (MODULUS 3, REMAINDER 2);

CREATE TABLE local_ref_p0(bid int, aid int, bval text);
CREATE TABLE local_ref_p1(bid int, aid int, bval text);
CREATE TABLE ref (bid int, aid int, bval text) PARTITION BY HASH(aid);

CREATE FOREIGN TABLE ref_p0 PARTITION OF ref FOR VALUES WITH (MODULUS 3, REMAINDER 0)
	SERVER loopback OPTIONS (table_name 'local_ref_p0');
CREATE FOREIGN TABLE ref_p1 PARTITION OF ref FOR VALUES WITH (MODULUS 3, REMAINDER 1)
	SERVER loopback OPTIONS (table_name 'local_ref_p1');
CREATE TABLE ref_p2 PARTITION OF ref FOR VALUES WITH (MODULUS 3, REMAINDER 2);

INSERT INTO main SELECT i, 'val_' || i FROM generate_series(1,100) i;
INSERT INTO ref SELECT i, mod(i, 10) + 1, 'val_' || i FROM generate_series(1,1000) i;

ANALYZE local_main_p0, local_main_p1, main_p2;
ANALYZE local_ref_p0, local_ref_p1, ref_p2;

SELECT str AS result
FROM expln('
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * from main AS a, ref AS b
WHERE a.aid = b.aid AND b.bval like ''val%''') AS str
WHERE str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * from main AS a, ref AS b
WHERE a.aid = b.aid AND b.bval like ''val%''') AS str
WHERE str NOT LIKE '%Memory%';

DROP TABLE main, local_main_p0, local_main_p1;
DROP TABLE ref, local_ref_p0, local_ref_p1;
ALTER SERVER loopback OPTIONS (DROP fdw_tuple_cost);
reset enable_partitionwise_join;

-- TODO: Non-mergejoinable join condition.
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM frgn AS a, frgn AS b WHERE a.x<b.x;
SELECT str FROM expln('
  EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, VERBOSE)
    SELECT * FROM frgn AS a, frgn AS b WHERE a.x<b.x;
') AS str;

DROP EXTENSION aqo CASCADE;
DROP EXTENSION postgres_fdw CASCADE;
DROP TABLE local;
DROP TABLE local_b;
DROP TABLE local_a;
