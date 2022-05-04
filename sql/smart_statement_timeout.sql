-- The function just copied from stats_ext.sql
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

SET statement_timeout = 5000; -- [0.8s]

DROP TABLE IF EXISTS a,b CASCADE;
CREATE TABLE a (x int);
INSERT INTO a (x) SELECT mod(ival,10) FROM generate_series(1,1000) As ival;

CREATE TABLE b (y int);
INSERT INTO b (y) SELECT mod(ival + 1,10) FROM generate_series(1,1000) As ival;

CREATE EXTENSION IF NOT EXISTS aqo;
SET aqo.join_threshold = 0;
SET aqo.mode = 'learn';
SET aqo.show_details = 'off';
SET aqo.learn_statement_timeout = 'on';
SET aqo.statement_timeout = 4; -- [0.8s]
SELECT check_estimated_rows('SELECT x FROM A,B where x < 10 and y > 10 group by(x);'); -- haven't any partial data
select flex_timeout, count_increase_timeout from aqo_queries where query_hash <> 0;
SELECT check_estimated_rows('SELECT x FROM A,B where x < 10 and y > 10 group by(x);'); -- haven't any partial data
select flex_timeout, count_increase_timeout from aqo_queries where query_hash <> 0;
SELECT check_estimated_rows('SELECT x FROM A,B where x < 10 and y > 10 group by(x);'); -- haven't any partial data
select flex_timeout, count_increase_timeout from aqo_queries where query_hash <> 0;
SELECT check_estimated_rows('SELECT x FROM A,B where x < 10 and y > 10 group by(x);'); -- haven't any partial data
