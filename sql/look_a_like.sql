-- Preliminaries
CREATE EXTENSION IF NOT EXISTS aqo;
SELECT true AS success FROM aqo_reset();

SET aqo.wide_search = 'on';

SET aqo.mode = 'learn';
SET aqo.show_details = 'on';
SET aqo.show_hash = 'off';
SET aqo.min_neighbors_for_predicting = 1;
SET aqo.predict_with_few_neighbors = 'off';
SET enable_nestloop = 'off';
SET enable_mergejoin = 'off';
SET enable_material = 'off';

DROP TABLE IF EXISTS a,b CASCADE;

-- Create tables with correlated datas in columns
CREATE TABLE a (x1 int, x2 int, x3 int);
INSERT INTO a (x1, x2, x3) SELECT mod(ival,10), mod(ival,10), mod(ival,10) FROM generate_series(1,100) As ival;

CREATE TABLE b (y1 int, y2 int, y3 int);
INSERT INTO b (y1, y2, y3) SELECT mod(ival + 1,10), mod(ival + 1,10), mod(ival + 1,10) FROM generate_series(1,1000) As ival;

ANALYZE a, b;

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
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A LEFT JOIN b ON A.x1 = B.y1 WHERE x1 = 5 AND x2 = 5;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 < 5 AND x2 < 5 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

--query contains nodes that have already been predicted

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 < 10 AND x2 < 5 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 > 2 AND x2 > 2 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 > 5 AND x2 > 5 AND x3 < 10 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 < 5 AND x2 < 5 AND x3 < 10 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

--query contains nodes that have already been predicted

SELECT str AS result
FROM expln('
SELECT x1,y1 FROM A,B WHERE x1 < 5 AND x2 < 4 AND x3 < 5 AND A.x1 = B.y1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 4 AND x3 > 1 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

--query contains nodes that have already been predicted

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 4 AND x3 > 1 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 4 AND x3 > 2 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 3 AND x2 < 5 AND x3 > 1 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 > 1 AND x2 < 4 AND x3 > 1 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 > 1 AND x2 < 4 AND x3 < 5 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 4 AND x2 < 5 AND x3 > 1 and y1 > 2 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

--query contains nodes that have already been predicted

SELECT str AS result
FROM expln('
SELECT x1 FROM A,B WHERE x1 < 3 AND x2 < 4 AND x3 > 1 and y1 > 2 GROUP BY(x1);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

CREATE TABLE c (z1 int, z2 int, z3 int);
INSERT INTO c (z1, z2, z3) SELECT mod(ival + 1,10), mod(ival + 1,10), mod(ival + 1,10) FROM generate_series(1,1000) As ival;

ANALYZE c;

SELECT str AS result
FROM expln('
SELECT * FROM (a LEFT JOIN b ON a.x1 = b.y1) sc WHERE
not exists (SELECT z1 FROM c WHERE sc.x1=c.z1 );') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

SELECT str AS result
FROM expln('
SELECT * FROM (A LEFT JOIN B ON A.x1 = B.y1) sc left join C on sc.x1=C.z1;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';


-- Next few test cases focus on fss corresponding to (x1 > ? AND x2 < ? AND x3 < ?). We will denote
-- it by fss0. At this moment there is exactly one fs with (fs, fss0, dbid) record in aqo_data. We'll
-- refer to it as fs0.

-- Let's create another fs for fss0. We'll call this fs fs1. Since aqo.wide_search='on',
-- aqo.min_neighbors_for_predicting=1, and there is (fs0, fss0, dbid) data record, AQO must be used here.
SELECT str AS result
FROM expln('
SELECT * FROM A WHERE x1 > -100 AND x2 < 10 AND x3 < 10;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';
-- Now there are 2 data records for fss0: one for (fs0, fss0, dbid) and one for (fs1, fss0, dbid)

-- We repeat previous query, but set aqo.min_neighbors_for_predicting to 2. Since aqo.predict_with_few_neighbors
-- is 'off', AQO is obliged to use both data records for fss0.
SET aqo.min_neighbors_for_predicting = 2;
SELECT str AS result
FROM expln('
SELECT * FROM A WHERE x1 > 1 AND x2 < 10 AND x3 < 10;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';
-- Now there are 3 data records for fss0: 1 for (fs0, fss0, dbid) and 2 for (fs1, fss0, dbid)

-- Lastly, we run invoke query with previously unseen fs with fss0 feature subspace. AQO must use
-- three data records from two neighbors for this one.
SET aqo.min_neighbors_for_predicting = 3;
SELECT str AS result
FROM expln('
SELECT x2 FROM A WHERE x1 > 3 AND x2 < 10 AND x3 < 10 GROUP BY(x2);') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';

-----
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT x, x AS y, x AS z FROM generate_series(1, 10000) x;
ANALYZE t;
SELECT true AS success FROM aqo_reset();

-- Test that when there are less records than aqo.min_neighbors_for_predicting for given (fs, fss, dbid)
-- and aqo.predict_with_few_neighbors is off, those records have higher precedence for cardinality estimation
-- than neighbors' records.
SELECT str AS result
FROM expln('
select * from t where x <= 10000 and y <= 10000 and z <= 10000;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';
DO
$$
BEGIN
	for counter in 1..20 loop
		EXECUTE format('explain analyze select *, 1 from t where x <= 1 and y <= 1 and z <= %L;', 10 * counter);
		EXECUTE format('explain analyze select *, 1 from t where x <= 1 and y <= %L and z <= 1;', 10 * counter);
		EXECUTE format('explain analyze select *, 1 from t where x <= %L and y <= 1 and z <= 1;', 10 * counter);
	end loop;
END;
$$ LANGUAGE PLPGSQL;
-- AQO should predict ~1000 rows to indicate that the record from previous invocation was used.
SELECT str AS result
FROM expln('
select * from t where x <= 10000 and y <= 10000 and z <= 10000;') AS str
WHERE str NOT LIKE 'Query Identifier%' and str NOT LIKE '%Memory%' and str NOT LIKE '%Sort Method%';


RESET aqo.wide_search;
RESET aqo.predict_with_few_neighbors;
RESET aqo.min_neighbors_for_predicting;
DROP EXTENSION aqo CASCADE;

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
DROP TABLE t;
DROP FUNCTION expln;
