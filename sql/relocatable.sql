CREATE EXTENSION aqo;
SET aqo.join_threshold = 0;
SET aqo.mode = 'learn'; -- use this mode for unconditional learning

CREATE TABLE test AS (SELECT id, 'payload' || id FROM generate_series(1,100) id);
ANALYZE test;

-- Learn on a query
SELECT count(*) FROM test;
SELECT query_text, learn_aqo, use_aqo, auto_tuning
FROM aqo_query_texts aqt JOIN aqo_queries aq ON (aqt.queryid = aq.queryid)
ORDER BY (md5(query_text))
; -- Check result. TODO: use aqo_status()

-- Create a schema and move AQO into it.
CREATE SCHEMA IF NOT EXISTS test;
ALTER EXTENSION aqo SET SCHEMA test;

-- Do something to be confident that AQO works
SELECT count(*) FROM test;
SELECT count(*) FROM test WHERE id < 10;

SELECT query_text, learn_aqo, use_aqo, auto_tuning
FROM test.aqo_query_texts aqt JOIN test.aqo_queries aq ON (aqt.queryid = aq.queryid)
ORDER BY (md5(query_text))
; -- Find out both queries executed above

-- Add schema which contains AQO to the end of search_path
SELECT set_config('search_path', current_setting('search_path') || ', test', false);

SELECT count(*) FROM test;
SELECT count(*) FROM test WHERE id < 10;

SELECT query_text, learn_aqo, use_aqo, auto_tuning
FROM test.aqo_query_texts aqt JOIN test.aqo_queries aq ON (aqt.queryid = aq.queryid)
ORDER BY (md5(query_text))
; -- Check result.

/*
 * Below, we should check each UI function
 */
SELECT aqo_disable_query(id) FROM (
  SELECT queryid AS id FROM aqo_queries WHERE queryid <> 0) AS q1;
SELECT learn_aqo, use_aqo, auto_tuning FROM test.aqo_queries
ORDER BY (learn_aqo, use_aqo, auto_tuning);
SELECT aqo_enable_query(id) FROM (
  SELECT queryid AS id FROM aqo_queries WHERE queryid <> 0) AS q1;
SELECT learn_aqo, use_aqo, auto_tuning FROM test.aqo_queries
ORDER BY (learn_aqo, use_aqo, auto_tuning);

RESET search_path;
DROP TABLE test CASCADE;
DROP SCHEMA IF EXISTS test CASCADE;
DROP EXTENSION IF EXISTS aqo CASCADE;
