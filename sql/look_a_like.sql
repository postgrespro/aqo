CREATE EXTENSION aqo;
SET aqo.join_threshold = 0;
SET aqo.mode = 'learn';
SET aqo.show_details = 'on';

SET enable_material = 'off';

DROP TABLE IF EXISTS a,b CASCADE;
CREATE TABLE a (x int);
INSERT INTO a (x) SELECT mod(ival,10) FROM generate_series(1,1000) As ival;

CREATE TABLE b (y int);
INSERT INTO b (y) SELECT mod(ival + 1,10) FROM generate_series(1,1000) As ival;

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

-- no one predicted rows. we use knowledge cardinalities of the query
-- in the next queries with the same fss_hash
SELECT str AS result
FROM expln('
SELECT x FROM A where x = 5;') AS str;

SELECT str AS result
FROM expln('
SELECT x FROM A,B WHERE x = 5 AND A.x = B.y;') AS str
; -- Find cardinality for SCAN A(x=5) from a neighbour class, created by the
-- query, executed above.

SELECT str AS result
FROM expln('
SELECT x, sum(x) FROM A,B WHERE y = 5 AND A.x = B.y group by(x);') AS str
; -- Find the JOIN cardinality from a neighbour class.

-- cardinality 100 in the first Seq Scan on a
SELECT str AS result
FROM expln('
SELECT x, sum(x) FROM A WHERE x = 5 group by(x);') AS str;

-- no one predicted rows. we use knowledge cardinalities of the query
-- in the next queries with the same fss_hash
SELECT str AS result
FROM expln('
SELECT x FROM A where x < 10 group by(x);') AS str
WHERE str NOT LIKE '%Memory%';
-- cardinality 1000 in Seq Scan on a
SELECT str AS result
FROM expln('
SELECT x,y FROM A,B WHERE x < 10 AND A.x = B.y;') AS str
WHERE str NOT LIKE '%Memory%';

-- cardinality 100 in Seq Scan on a and Seq Scan on b
SELECT str AS result
FROM expln('
SELECT x FROM A,B where x < 10 and y > 10 group by(x);') AS str
WHERE str NOT LIKE '%Memory%';

--
-- TODO:
-- Not executed case. What could we do better here?
--
SELECT str AS result
FROM expln('
SELECT x,y FROM A,B WHERE x < 10 and y > 10 AND A.x = B.y;') AS str
WHERE str NOT LIKE '%Memory%'
;

RESET enable_material;
DROP TABLE a,b CASCADE;
SELECT true FROM aqo_reset();
DROP EXTENSION aqo CASCADE;
