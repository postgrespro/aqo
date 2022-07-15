CREATE EXTENSION aqo;
SET aqo.join_threshold = 0;
SET aqo.mode = 'learn';

CREATE TEMP TABLE tt();
CREATE TABLE pt();

-- Ignore queries with the only temp tables
SELECT count(*) FROM tt;
SELECT count(*) FROM tt AS t1, tt AS t2;
SELECT query_text FROM aqo_query_texts; -- Default row should be returned

-- Should be stored in the ML base
SELECT count(*) FROM pt;
SELECT count(*) FROM pt, tt;
SELECT count(*) FROM pt AS pt1, tt AS tt1, tt AS tt2, pt AS pt2;
SELECT count(*) FROM aqo_data; -- Don't bother about false negatives because of trivial query plans

DROP TABLE tt;
SELECT * FROM aqo_cleanup();
SELECT count(*) FROM aqo_data; -- Should return the same as previous call above
DROP TABLE pt;
SELECT * FROM aqo_cleanup();
SELECT count(*) FROM aqo_data; -- Should be 0
SELECT query_text FROM aqo_queries aq LEFT JOIN aqo_query_texts aqt
ON aq.queryid = aqt.queryid
ORDER BY (md5(query_text)); -- The only the common class is returned

-- Test learning on temporary table
CREATE TABLE pt AS SELECT x AS x, (x % 10) AS y FROM generate_series(1,100) AS x;
CREATE TEMP TABLE tt AS SELECT -x AS x, (x % 7) AS y FROM generate_series(1,100) AS x;
CREATE TEMP TABLE ttd AS -- the same structure as tt
  SELECT -(x*3) AS x, (x % 9) AS y FROM generate_series(1,100) AS x;
ANALYZE pt,tt,ttd;

create function check_estimated_rows(text) returns table (estimated int, actual int)
language plpgsql as
$$
declare
    ln text;
    tmp text[];
    first_row bool := true;
begin
    for ln in
        execute format('explain analyze %s', $1)
    loop
        if first_row then
            first_row := false;
            tmp := regexp_match(ln, 'rows=(\d*) .* rows=(\d*)');
            return query select tmp[1]::int, tmp[2]::int;
        end if;
    end loop;
end;
$$;

-- Check: AQO learns on queries with temp tables

SELECT * FROM check_estimated_rows('
	SELECT pt.x, avg(pt.y) FROM pt,tt  WHERE pt.x = tt.x GROUP BY (pt.x);
'); -- Estimation failed. Learn.
SELECT * FROM check_estimated_rows('
	SELECT pt.x, avg(pt.y) FROM pt,tt  WHERE pt.x = tt.x GROUP BY (pt.x);
'); -- Should use AQO estimation
SELECT * FROM check_estimated_rows('
	SELECT pt.x, avg(pt.y) FROM pt,ttd  WHERE pt.x = ttd.x GROUP BY (pt.x);
'); -- Should use AQO estimation with another temp table of the same structure

SET aqo.mode = 'forced'; -- Now we use all fss records for each query
DROP TABLE pt;
SELECT * FROM aqo_cleanup();
CREATE TABLE pt AS SELECT x AS x, (x % 10) AS y FROM generate_series(1,100) AS x;
CREATE TEMP TABLE ttd1 AS
  SELECT -(x*3) AS x, (x % 9) AS y1 FROM generate_series(1,100) AS x;
ANALYZE;

-- Check: use AQO knowledge with different temp table of the same structure

SELECT * FROM check_estimated_rows('
	SELECT pt.x, avg(pt.y) FROM pt,tt  WHERE pt.x = tt.x GROUP BY (pt.x);
'); -- Estimation failed. Learn.
SELECT * FROM check_estimated_rows('
	SELECT pt.x, avg(pt.y) FROM pt,tt  WHERE pt.x = tt.x GROUP BY (pt.x);
'); -- Should use AQO estimation
SELECT * FROM check_estimated_rows('
	SELECT pt.x, avg(pt.y) FROM pt,ttd  WHERE pt.x = ttd.x GROUP BY (pt.x);
'); -- Should use AQO estimation with another temp table of the same structure
SELECT * FROM check_estimated_rows('
	SELECT pt1.x, avg(pt1.y) FROM pt AS pt1,ttd  WHERE pt1.x = ttd.x GROUP BY (pt1.x);
'); -- Alias doesn't influence feature space
SELECT * FROM check_estimated_rows('
	SELECT pt.x, avg(pt.y) FROM pt,ttd1  WHERE pt.x = ttd1.x GROUP BY (pt.x);
'); -- Don't use AQO for temp table because of different attname

DROP TABLE pt CASCADE;
SELECT 1 FROM aqo_reset();
DROP EXTENSION aqo;
DROP FUNCTION check_estimated_rows;
