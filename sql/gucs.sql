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

CREATE EXTENSION aqo;
SET aqo.log_ignorance = 'off';
SET aqo.log_ignorance = 'on';
SET aqo.log_ignorance = 'on';
\d aqo_ignorance

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM t;
SELECT node_type FROM aqo_ignorance;

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM t;
SELECT node_type FROM aqo_ignorance;

-- This GUC can be changed by an admin only.
CREATE ROLE noadmin;
SET ROLE noadmin;
SET aqo.log_ignorance = 'off';
RESET ROLE;

DROP EXTENSION aqo;
