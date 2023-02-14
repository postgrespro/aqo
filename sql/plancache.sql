-- Tests on interaction of AQO with cached plans.

CREATE EXTENSION IF NOT EXISTS aqo;
SELECT true AS success FROM aqo_reset();

SET aqo.mode = 'intelligent';
SET aqo.show_details = 'on';
SET aqo.show_hash = 'off';

CREATE TABLE test AS SELECT x FROM generate_series(1,10) AS x;
ANALYZE test;

-- Function which implements a test where AQO is used for both situations where
-- a query is planned or got from a plan cache.
-- Use a function to hide a system dependent hash value.
CREATE FUNCTION f1() RETURNS TABLE (
  nnex	bigint,
  nex	bigint,
  pt	double precision[]
) AS $$
DECLARE
  i				integer;
  qhash			bigint;
BEGIN
  PREPARE fooplan (int) AS SELECT count(*) FROM test WHERE x = $1;

  FOR i IN 1..10 LOOP
    execute 'EXECUTE fooplan(1)';
  END LOOP;

  SELECT queryid FROM aqo_query_texts
    WHERE query_text LIKE '%count(*) FROM test WHERE x%' INTO qhash;

  RETURN QUERY SELECT executions_without_aqo nnex,
  					  executions_with_aqo nex,
  					  planning_time_with_aqo pt
    FROM  aqo_query_stat WHERE queryid = qhash;
END $$ LANGUAGE 'plpgsql';

-- The function shows 6 executions without an AQO support (nnex) and
-- 4 executions with usage of an AQO knowledge base (nex). Planning time in the
-- case of AQO support (pt) is equal to '-1', because the query plan is extracted
-- from the plan cache.
SELECT * FROM f1();

DROP FUNCTION f1;
DROP TABLE test CASCADE;

DROP EXTENSION aqo;
