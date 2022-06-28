CREATE EXTENSION aqo;
SET aqo.join_threshold = 0;
SET aqo.mode = 'learn';
SET aqo.show_details = 'on';
set aqo.show_hash = 'off';
SET aqo.k_neighbors_threshold_for_predict = 1;

SET enable_material = 'off';

DROP TABLE IF EXISTS a,b CASCADE;

-- Create tables with correlated datas in columns
CREATE TABLE a (x1 int, x2 int, x3 int);
INSERT INTO a (x1, x2, x3) SELECT mod(ival,10), mod(ival,10), mod(ival,10) FROM generate_series(1,1000) As ival;

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

-- no one predicted rows. we use knowledge cardinalities of the query
-- in the next queries with the same fss_hash

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 = 5 AND x2 = 5 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 < 5 AND x2 < 5 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 < 5 AND x2 = 5 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

--query contains nodes that have already been predicted

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 < 10 AND x2 < 5 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 > 2 AND x2 > 2 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 > 5 AND x2 > 5 AND x3 < 10 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 < 5 AND x2 < 5 AND x3 < 10 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

--query contains nodes that have already been predicted

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 < 5 AND x2 < 4 AND x3 < 5 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 4 AND x3 > 1 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

--query contains nodes that have already been predicted

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 4 AND x3 > 1 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 4 AND x3 > 2 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 3 AND x2 < 5 AND x3 > 1 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 > 1 AND x2 < 4 AND x3 > 1 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 > 1 AND x2 < 4 AND x3 < 5 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 4 AND x2 < 5 AND x3 > 1 and y1 > 2 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

--query contains nodes that have already been predicted

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 3 AND x2 < 4 AND x3 > 1 and y1 > 2 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%';

SELECT 1 FROM aqo_reset();
DROP TABLE a;
DROP TABLE b;
DROP EXTENSION aqo CASCADE;
