CREATE EXTENSION aqo;
SET aqo.mode = 'learn';
SET aqo.show_details = true;

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

CREATE TABLE t(x int);
INSERT INTO t (x) (SELECT * FROM generate_series(1, 100) AS gs);
ANALYZE t;

SELECT str AS result
FROM expln('SELECT x FROM t;', TRUE) AS str
WHERE str NOT LIKE 'Query Identifier%';
SELECT str AS result
FROM expln('SELECT x FROM t;', TRUE) AS str
WHERE str NOT LIKE 'Query Identifier%';

DROP EXTENSION aqo;

SET aqo.log_ignorance = 'on';
SET aqo.log_ignorance = 'off';
SET aqo.log_ignorance = 'off';
SET aqo.log_ignorance = 'on';

CREATE EXTENSION aqo;
SET aqo.log_ignorance = 'off';
SET aqo.log_ignorance = 'on';
SET aqo.log_ignorance = 'on';
\d aqo_ignorance

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM t;
SELECT node_type FROM aqo_ignorance;

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM t;
SELECT node_type FROM aqo_ignorance;

-- This GUC can be changed by an admin only.
CREATE ROLE noadmin;
SET ROLE noadmin;
SET aqo.log_ignorance = 'off';
RESET ROLE;

DROP EXTENSION aqo;
