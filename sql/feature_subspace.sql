-- This test related to some issues on feature subspace calculation

CREATE EXTENSION aqo;

SET aqo.mode = 'learn';
SET aqo.join_threshold = 0;
SET aqo.show_details = 'on';

CREATE TABLE a AS (SELECT gs AS x FROM generate_series(1,10) AS gs);
CREATE TABLE b AS (SELECT gs AS x FROM generate_series(1,100) AS gs);

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

--
-- A LEFT JOIN B isn't equal B LEFT JOIN A.
--
SELECT str AS result
FROM expln('
SELECT * FROM a LEFT JOIN b USING (x);') AS str
WHERE str NOT LIKE '%Memory%';

-- TODO: Using method of other classes neighbours we get a bad estimation.
SELECT str AS result
FROM expln('
SELECT * FROM b LEFT JOIN a USING (x);') AS str
WHERE str NOT LIKE '%Memory%';

-- Look into the reason: two JOINs from different classes have the same FSS.
SELECT to_char(d1.targets[1], 'FM999.00') AS target FROM aqo_data d1
JOIN aqo_data d2 ON (d1.fs <> d2.fs AND d1.fss = d2.fss)
WHERE 'a'::regclass = ANY (d1.oids) AND 'b'::regclass = ANY (d1.oids) order by target;

DROP TABLE a,b CASCADE;
SELECT true FROM aqo_reset();
DROP EXTENSION aqo;
