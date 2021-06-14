CREATE EXTENSION aqo;
SET aqo.mode = 'learn';
SET aqo.show_details = true;

CREATE TABLE t(x int);
INSERT INTO t (x) (SELECT * FROM generate_series(1, 100) AS gs);
ANALYZE t;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
	SELECT x FROM t;
EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
	SELECT x FROM t;

DROP EXTENSION aqo;

SET aqo.log_ignorance = 'on';
SET aqo.log_ignorance = 'off';
SET aqo.log_ignorance = 'off';
SET aqo.log_ignorance = 'on';
\d aqo_ignorance
CREATE EXTENSION aqo;
SET aqo.log_ignorance = 'off';
SET aqo.log_ignorance = 'on';
SET aqo.log_ignorance = 'on';
\d aqo_ignorance

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
	SELECT * FROM t;
SELECT count(*) FROM t;
SELECT * FROM t ORDER BY (x) LIMIT 1;
(SELECT * FROM t LIMIT 1) UNION ALL (SELECT * FROM t LIMIT 1); -- Append must be included in ignorance table for now
SELECT node_type FROM aqo_ignorance; -- See ignorance table

-- Just repeat
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM t;
SELECT count(*) FROM t;
SELECT * FROM t ORDER BY (x) LIMIT 1;
(SELECT * FROM t LIMIT 1) UNION ALL (SELECT * FROM t LIMIT 1);
SELECT node_type FROM aqo_ignorance; -- See the ignorance table. There shouldn't be Sort and Agg nodes.
-- TODO: The SeqScan node got into the ignorance table: at planning stage we
-- don't know anything about it and made negative prediction. But on the
-- learning stage we wrote into fss table on learning on the first scan node.
-- Second scan node was detected as an abnormal node.

-- This GUC can be changed by an admin only.
CREATE ROLE noadmin;
SET ROLE noadmin;
SET aqo.log_ignorance = 'off';
RESET ROLE;

SET aqo.query_text_limit = 35;
SELECT count(*) FROM t WHERE x < 1; -- Crop query text

SET aqo.query_text_limit = 0;
SELECT count(*) FROM t WHERE x > 1; -- Store full query text

SET aqo.query_text_limit = 2147483647;
SELECT count(*) FROM t WHERE x = 1; -- ERROR: invalid memory alloc

SET aqo.query_text_limit = 8192;
SELECT count(*) FROM t WHERE x = 1; -- Store full query text

-- See stored query texts
SELECT query_text FROM aqo_query_texts ORDER BY md5(query_text);

DROP EXTENSION aqo;
