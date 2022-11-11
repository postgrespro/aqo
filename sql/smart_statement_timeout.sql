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
SET statement_timeout = 1500; -- [1.5s]
SET aqo.statement_timeout = 500; -- [0.5s]

SELECT count(a.x1),count(B.y1) FROM A a LEFT JOIN B ON a.x1 = B.y1 LEFT JOIN A a1 ON a1.x1 = B.y1;
select smart_timeout, count_increase_timeout from aqo_queries, aqo_query_texts
        where query_text = 'SELECT count(a.x1),count(B.y1) FROM A a LEFT JOIN B ON a.x1 = B.y1 LEFT JOIN A a1 ON a1.x1 = B.y1;'
        and aqo_query_texts.queryid = aqo_queries.queryid limit 1;

SET aqo.learn_statement_timeout = 'off';
SET aqo.statement_timeout = 1000; -- [1s]
INSERT INTO a (x1, x2, x3) SELECT mod(ival,20), mod(ival,10), mod(ival,10) FROM generate_series(1,1000) As ival;
SET aqo.learn_statement_timeout = 'on';
SET aqo.statement_timeout = 500; -- [0.5s]
SELECT count(a.x1),count(B.y1) FROM A a LEFT JOIN B ON a.x1 = B.y1 LEFT JOIN A a1 ON a1.x1 = B.y1;
select smart_timeout, count_increase_timeout from aqo_queries, aqo_query_texts
        where query_text = 'SELECT count(a.x1),count(B.y1) FROM A a LEFT JOIN B ON a.x1 = B.y1 LEFT JOIN A a1 ON a1.x1 = B.y1;'
        and aqo_query_texts.queryid = aqo_queries.queryid limit 1;
SELECT count(a.x1),count(B.y1) FROM A a LEFT JOIN B ON a.x1 = B.y1 LEFT JOIN A a1 ON a1.x1 = B.y1;
select smart_timeout, count_increase_timeout from aqo_queries, aqo_query_texts
        where query_text = 'SELECT count(a.x1),count(B.y1) FROM A a LEFT JOIN B ON a.x1 = B.y1 LEFT JOIN A a1 ON a1.x1 = B.y1;'
        and aqo_query_texts.queryid = aqo_queries.queryid limit 1;

SET statement_timeout = 100; -- [0.1s]
SET aqo.statement_timeout = 150;
SELECT count(a.x1),count(B.y1) FROM A a LEFT JOIN B ON a.x1 = B.y1 LEFT JOIN A a1 ON a1.x1 = B.y1;
select smart_timeout, count_increase_timeout from aqo_queries, aqo_query_texts
        where query_text = 'SELECT count(a.x1),count(B.y1) FROM A a LEFT JOIN B ON a.x1 = B.y1 LEFT JOIN A a1 ON a1.x1 = B.y1;'
        and aqo_query_texts.queryid = aqo_queries.queryid limit 1;

SELECT 1 FROM aqo_reset();
DROP TABLE a;
DROP TABLE b;
DROP EXTENSION aqo;