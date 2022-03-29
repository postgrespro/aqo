DROP EXTENSION IF EXISTS aqo CASCADE;
DROP SCHEMA IF EXISTS test CASCADE;

CREATE EXTENSION aqo;
SET aqo.mode = 'intelligent';

CREATE TABLE test (id SERIAL, data TEXT);
INSERT INTO test (data) VALUES ('string');
SELECT * FROM test;

SELECT query_text FROM aqo_query_texts;
SELECT learn_aqo, use_aqo, auto_tuning FROM aqo_queries;

CREATE SCHEMA IF NOT EXISTS test;
ALTER EXTENSION aqo SET SCHEMA test;

SET aqo.mode = 'intelligent';

CREATE TABLE test1 (id SERIAL, data TEXT);
INSERT INTO test1 (data) VALUES ('string');
SELECT * FROM test1;

SELECT query_text FROM test.aqo_query_texts;
SELECT learn_aqo, use_aqo, auto_tuning FROM test.aqo_queries;

SET search_path TO test;

CREATE TABLE test2 (id SERIAL, data TEXT);
INSERT INTO test2 (data) VALUES ('string');
SELECT * FROM test2;

SELECT query_text FROM aqo_query_texts;
SELECT learn_aqo, use_aqo, auto_tuning FROM aqo_queries;
DROP SCHEMA IF EXISTS test CASCADE;
DROP EXTENSION IF EXISTS aqo CASCADE;

SET search_path TO public;