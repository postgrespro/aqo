DROP EXTENSION IF EXISTS aqo CASCADE;
DROP SCHEMA IF EXISTS test CASCADE;

-- Check Zero-schema path behaviour
CREATE SCHEMA IF NOT EXISTS test;
SET search_path TO test;
DROP SCHEMA IF EXISTS test CASCADE;
CREATE EXTENSION aqo;  -- fail

-- Check default schema switching after AQO initialization
CREATE SCHEMA IF NOT EXISTS test1;
SET search_path TO test1, public;
CREATE EXTENSION aqo;
SET aqo.join_threshold = 0;
SET aqo.mode = 'intelligent';

CREATE TABLE test (id SERIAL, data TEXT);
INSERT INTO test (data) VALUES ('string');
SELECT * FROM test;

-- Check AQO service relations state after some manipulations
-- Exclude fields with hash values from the queries. Hash is depend on
-- nodefuncs code which is highly PostgreSQL version specific.
SELECT query_text FROM aqo_query_texts
ORDER BY (md5(query_text)) DESC;
SELECT learn_aqo, use_aqo, auto_tuning FROM aqo_queries
ORDER BY (learn_aqo, use_aqo, auto_tuning);
DROP SCHEMA IF EXISTS test1 CASCADE;
