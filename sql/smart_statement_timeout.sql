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

SELECT count(y), pg_sleep(3) FROM a,b where x > 2 * (select min(x) from A,B where x = y);
select * from aqo_queries where query_hash <> 0;
SELECT count(y), pg_sleep(3) FROM a,b where x > 3 * (select min(x) from A,B where x = y);
select * from aqo_queries where query_hash <> 0;
SELECT count(y), pg_sleep(3) FROM a,b where x > 3 * (select min(x) from A,B where x = y);
select * from aqo_queries where query_hash <> 0;
SELECT count(y), pg_sleep(3) FROM a,b where x > (select min(x) from A,B where x = y);
select * from aqo_queries where query_hash <> 0;
SELECT count(y), pg_sleep(3) FROM a,b where x > (select min(x) from A,B where x = y);
select * from aqo_queries where query_hash <> 0;
SELECT count(y), pg_sleep(3) FROM a,b where x > (select min(x) from A,B where x = y);
select * from aqo_queries where query_hash <> 0;
DROP EXTENSION aqo;
