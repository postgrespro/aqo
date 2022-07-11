DROP TABLE IF EXISTS a,b CASCADE;
CREATE TABLE a (x1 int, x2 int, x3 int);
INSERT INTO a (x1, x2, x3) SELECT mod(ival,4), mod(ival,10), mod(ival,10) FROM generate_series(1,100) As ival;

CREATE TABLE b (y1 int, y2 int, y3 int);
INSERT INTO b (y1, y2, y3) SELECT mod(ival + 1,4), mod(ival + 1,10), mod(ival + 1,10) FROM generate_series(1,100) As ival;

CREATE EXTENSION IF NOT EXISTS aqo;
SET aqo.join_threshold = 0;
SET aqo.mode = 'learn';
SET aqo.show_details = 'off';
SET aqo.learn_statement_timeout = 'on';
SET statement_timeout = 2500; -- [2.5s]
SET aqo.statement_timeout = 5000;

SELECT pg_sleep(2),count(x1),count(y1) FROM A,B WHERE x1 = 5 AND x2 = 5 AND A.x1 = B.y1;
select queryid, smart_timeout, count_increase_timeout from aqo_queries where queryid <> 0 and count_increase_timeout > 0 limit 1;
INSERT INTO a (x1, x2, x3) SELECT mod(ival,20), mod(ival,10), mod(ival,10) FROM generate_series(1,1000) As ival;
SELECT pg_sleep(2),count(x1),count(y1) FROM A,B WHERE x1 = 5 AND x2 = 5 AND A.x1 = B.y1;
select queryid, smart_timeout, count_increase_timeout from aqo_queries where queryid <> 0 and count_increase_timeout > 1 limit 1;
SELECT pg_sleep(2),count(x1),count(y1) FROM A,B WHERE x1 = 5 AND x2 = 5 AND A.x1 = B.y1;
select queryid, smart_timeout, count_increase_timeout from aqo_queries where queryid <> 0 and count_increase_timeout > 2 limit 1;

SET aqo.statement_timeout = 1500; -- [1.5s]
SELECT pg_sleep(1),count(x1),count(y1) FROM A,B WHERE x1 > 5 AND x2 > 5 AND x3 < 10 AND A.x1 = B.y1;
INSERT INTO b (y1, y2, y3) SELECT mod(ival,20), mod(ival,10), mod(ival,10) FROM generate_series(1,1000) As ival;
SELECT pg_sleep(1),count(x1),count(y1) FROM A,B WHERE x1 > 5 AND x2 > 5 AND x3 < 10 AND A.x1 = B.y1;
select queryid, smart_timeout, count_increase_timeout from aqo_queries where queryid <> 0 and count_increase_timeout > 1 limit 2;
SELECT pg_sleep(1),count(x1),count(y1) FROM A,B WHERE x1 > 5 AND x2 > 5 AND x3 < 10 AND A.x1 = B.y1;
select queryid, smart_timeout, count_increase_timeout from aqo_queries where queryid <> 0 and count_increase_timeout > 2 limit 2;

SET aqo.statement_timeout = 15;
SELECT pg_sleep(1),count(x1),count(y1) FROM A,B WHERE x1 > 5 AND x2 > 5 AND x3 < 10 AND A.x1 = B.y1;
select queryid, smart_timeout, count_increase_timeout from aqo_queries where queryid <> 0 and count_increase_timeout > 2 limit 2;

SELECT 1 FROM aqo_reset();
DROP EXTENSION aqo;
