-- This test related to some issues on feature subspace calculation

CREATE EXTENSION aqo;

SET aqo.mode = 'learn';
SET aqo.join_threshold = 0;
SET aqo.show_details = 'on';

CREATE TABLE a AS (SELECT gs AS x FROM generate_series(1,10) AS gs);
CREATE TABLE b AS (SELECT gs AS x FROM generate_series(1,100) AS gs);

--
-- A LEFT JOIN B isn't equal B LEFT JOIN A.
--

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM a LEFT JOIN b USING (x);

-- TODO: Using method of other classes neighbours we get a bad estimation.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM b LEFT JOIN a USING (x);

-- Look into the reason: two JOINs from different classes have the same FSS.
SELECT to_char(d1.targets[1], 'FM999.00') AS target FROM aqo_data d1
JOIN aqo_data d2 ON (d1.fs <> d2.fs AND d1.fss = d2.fss)
WHERE 'a'::regclass = ANY (d1.oids) AND 'b'::regclass = ANY (d1.oids);

DROP TABLE a,b CASCADE;
SELECT true FROM aqo_reset();
DROP EXTENSION aqo;
