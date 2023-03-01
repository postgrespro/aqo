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

-- Preliminaries
CREATE EXTENSION IF NOT EXISTS aqo;
SELECT true AS success FROM aqo_reset();

CREATE TABLE t AS SELECT * FROM generate_series(1,50) AS x;
ANALYZE t;
DELETE FROM t WHERE x > 5; -- Force optimizer to make overestimated prediction.

SET aqo.mode = 'learn';
SET aqo.show_details = 'off';
SET aqo.learn_statement_timeout = 'on';

SET statement_timeout = 80; -- [0.1s]
SELECT *, pg_sleep(0.1) FROM t;

RESET statement_timeout;
SELECT check_estimated_rows('SELECT *, pg_sleep(0.1) FROM t;'); -- haven't any partial data

-- Don't learn because running node has smaller cardinality than an optimizer prediction
SET statement_timeout = 350;
SELECT *, pg_sleep(0.1) FROM t;

RESET statement_timeout;
SELECT check_estimated_rows('SELECT *, pg_sleep(0.1) FROM t;');

-- We have a real learning data.
SET statement_timeout = 800;
SELECT *, pg_sleep(0.1) FROM t;

RESET statement_timeout;
SELECT check_estimated_rows('SELECT *, pg_sleep(0.1) FROM t;');

-- Force to make an underestimated prediction
DELETE FROM t WHERE x > 2;
ANALYZE t;
INSERT INTO t (x) (SELECT * FROM generate_series(3,5) AS x);
SELECT true AS success FROM aqo_reset();

SET statement_timeout = 80;
SELECT *, pg_sleep(0.1) FROM t; -- Not learned

RESET statement_timeout;
SELECT check_estimated_rows('SELECT *, pg_sleep(0.1) FROM t;');

SET statement_timeout = 350;
SELECT *, pg_sleep(0.1) FROM t; -- Learn!

RESET statement_timeout;
SELECT check_estimated_rows('SELECT *, pg_sleep(0.1) FROM t;');

SET statement_timeout = 550;
SELECT *, pg_sleep(0.1) FROM t; -- Get reliable data

RESET statement_timeout;
SELECT check_estimated_rows('SELECT *, pg_sleep(0.1) FROM t;');

-- Interrupted query should immediately appear in aqo_data
SELECT true AS success FROM aqo_reset();
SET statement_timeout = 500;
SELECT count(*) FROM aqo_data; -- Must be zero
SELECT x, pg_sleep(0.1) FROM t WHERE x > 0;

RESET statement_timeout;
SELECT count(*) FROM aqo_data; -- Must be one

DROP TABLE t;
DROP FUNCTION check_estimated_rows;

SELECT true AS success FROM aqo_reset();
DROP EXTENSION aqo;
