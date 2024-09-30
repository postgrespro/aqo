-- Testing aqo_query_stat update logic
-- Note: this test assumes STAT_SAMPLE_SIZE to be 20.
CREATE EXTENSION IF NOT EXISTS aqo;
SELECT true AS success FROM aqo_reset();

DROP TABLE IF EXISTS A;
CREATE TABLE A AS SELECT x FROM generate_series(1, 20) as x;
ANALYZE A;

DROP TABLE IF EXISTS B;
CREATE TABLE B AS SELECT y FROM generate_series(1, 10) as y;
ANALYZE B;

CREATE OR REPLACE FUNCTION round_array (double precision[])
RETURNS double precision[]
LANGUAGE SQL
AS $$
   SELECT array_agg(round(elem::numeric, 3))
   FROM unnest($1) as arr(elem);
$$

SET aqo.mode = 'learn';
SET aqo.force_collect_stat = 'on';
SET aqo.min_neighbors_for_predicting = 1;

-- First test: adding real records
SET aqo.mode = 'disabled';
SELECT count(*) FROM A JOIN B ON (A.x > B.y) WHERE A.x > 15 AND B.y < 5;
SELECT count(*) FROM A JOIN B ON (A.x > B.y) WHERE A.x > 16 AND B.y < 6;

SET aqo.mode = 'learn';
SELECT aqo_enable_class(queryid) FROM aqo_queries WHERE queryid != 0;

SELECT count(*) FROM A JOIN B ON (A.x > B.y) WHERE A.x > 17 AND B.y < 7;
SELECT count(*) FROM A JOIN B ON (A.x > B.y) WHERE A.x > 18 AND B.y < 8;
SELECT count(*) FROM A JOIN B ON (A.x > B.y) WHERE A.x > 19 AND B.y < 9;
-- Ignore unstable time-related columns 
SELECT round_array(cardinality_error_with_aqo) AS error_aqo, round_array(cardinality_error_without_aqo) AS error_no_aqo, executions_with_aqo, executions_without_aqo FROM aqo_query_stat;

SELECT true AS success from aqo_reset();


-- Second test: fake data in aqo_query_stat
SET aqo.mode = 'disabled';
SELECT count(*) FROM A JOIN B ON (A.x > B.y) WHERE A.x > 5 AND B.y < 100;
SELECT aqo_query_stat_update(
	queryid,
	'{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}', '{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}',
	'{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}', '{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}',
	'{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}', '{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}',
	100, 50)
FROM aqo_query_stat;
SELECT round_array(cardinality_error_with_aqo) AS error_aqo, round_array(cardinality_error_without_aqo) AS error_no_aqo, executions_with_aqo, executions_without_aqo FROM aqo_query_stat;

SELECT count(*) FROM A JOIN B ON (A.x > B.y) WHERE A.x > 10 AND B.y < 100;

SET aqo.mode = 'learn';
SELECT aqo_enable_class(queryid) FROM aqo_queries WHERE queryid != 0;

SELECT count(*) FROM A JOIN B ON (A.x > B.y) WHERE A.x > 15 AND B.y < 5;
SELECT count(*) FROM A JOIN B ON (A.x > B.y) WHERE A.x > 16 AND B.y < 6;
SELECT count(*) FROM A JOIN B ON (A.x > B.y) WHERE A.x > 17 AND B.y < 7;
SELECT count(*) FROM A JOIN B ON (A.x > B.y) WHERE A.x > 18 AND B.y < 8;
SELECT round_array(cardinality_error_with_aqo) AS error_aqo, round_array(cardinality_error_without_aqo) AS error_no_aqo, executions_with_aqo, executions_without_aqo FROM aqo_query_stat;


SET aqo.mode TO DEFAULT;
SET aqo.force_collect_stat TO DEFAULT;
SET aqo.min_neighbors_for_predicting TO DEFAULT;

DROP FUNCTION round_array;
DROP TABLE A;
DROP TABLE B;
DROP EXTENSION aqo CASCADE;
