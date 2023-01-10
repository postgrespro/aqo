-- Check the learning-on-timeout feature
-- For stabilized reproduction autovacuum must be disabled.
CREATE FUNCTION check_estimated_rows(text) RETURNS TABLE (estimated int)
LANGUAGE plpgsql AS $$
DECLARE
    ln text;
    tmp text[];
    first_row bool := true;
BEGIN
    FOR ln IN
        execute format('explain %s', $1)
    LOOP
        IF first_row THEN
            first_row := false;
            tmp := regexp_match(ln, 'rows=(\d*)');
            RETURN QUERY SELECT tmp[1]::int;
        END IF;
    END LOOP;
END; $$;

CREATE TABLE t AS SELECT * FROM generate_series(1,100) AS x;
ANALYZE t;
DELETE FROM t WHERE x > 5; -- Force optimizer to make overestimated prediction.

CREATE EXTENSION IF NOT EXISTS aqo;
SET aqo.join_threshold = 0;
SET aqo.mode = 'learn';
SET aqo.show_details = 'off';
SET aqo.learn_statement_timeout = 'on';

SET statement_timeout = 800; -- [0.8s]
SELECT *, pg_sleep(1) FROM t;
SELECT check_estimated_rows('SELECT *, pg_sleep(1) FROM t;'); -- haven't any partial data

-- Don't learn because running node has smaller cardinality than an optimizer prediction
SET statement_timeout = 3500;
SELECT *, pg_sleep(1) FROM t;
SELECT check_estimated_rows('SELECT *, pg_sleep(1) FROM t;');

-- We have a real learning data.
SET statement_timeout = 10000;
SELECT *, pg_sleep(1) FROM t;
SELECT check_estimated_rows('SELECT *, pg_sleep(1) FROM t;');

-- Force to make an underestimated prediction
DELETE FROM t WHERE x > 2;
ANALYZE t;
INSERT INTO t (x) (SELECT * FROM generate_series(3,5) AS x);
SELECT 1 FROM aqo_reset();

SET statement_timeout = 800;
SELECT *, pg_sleep(1) FROM t; -- Not learned
SELECT check_estimated_rows('SELECT *, pg_sleep(1) FROM t;');

SET statement_timeout = 3500;
SELECT *, pg_sleep(1) FROM t; -- Learn!
SELECT check_estimated_rows('SELECT *, pg_sleep(1) FROM t;');

SET statement_timeout = 5500;
SELECT *, pg_sleep(1) FROM t; -- Get reliable data
SELECT check_estimated_rows('SELECT *, pg_sleep(1) FROM t;');

-- Interrupted query should immediately appear in aqo_data
SELECT 1 FROM aqo_reset();
SET statement_timeout = 500;
SELECT count(*) FROM aqo_data; -- Must be zero
SELECT x, pg_sleep(0.1) FROM t WHERE x > 0;
SELECT count(*) FROM aqo_data; -- Must be one

SELECT 1 FROM aqo_reset();
DROP TABLE t;
DROP EXTENSION aqo;
DROP FUNCTION check_estimated_rows;
