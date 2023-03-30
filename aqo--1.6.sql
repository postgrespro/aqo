/* contrib/aqo/aqo--1.6.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION aqo" to load this file. \quit

--
-- Get cardinality error of queries the last time they were executed.
-- IN:
-- controlled - show queries executed under a control of AQO (true);
-- executed without an AQO control, but AQO has a stat on the query (false).
--
-- OUT:
-- num - sequental number. Smaller number corresponds to higher error.
-- id - ID of a query.
-- fshash - feature space. Usually equal to zero or ID.
-- error - AQO error that calculated on plan nodes of the query.
-- nexecs - number of executions of queries associated with this ID.
--
CREATE FUNCTION aqo_cardinality_error(controlled boolean)
RETURNS TABLE(num integer, id bigint, fshash bigint, error double precision, nexecs bigint)
AS 'MODULE_PATHNAME', 'aqo_cardinality_error'
LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION aqo_cardinality_error(boolean) IS
'Get cardinality error of queries the last time they were executed. Order queries according to an error value.';

--
-- Remove unneeded rows from the AQO ML storage.
-- For common feature space, remove rows from aqo_data only.
-- For custom feature space - remove all rows related to the space from all AQO
-- tables even if only one oid for one feature subspace of the space is illegal.
-- Returns number of deleted rows from aqo_queries and aqo_data tables.
--
CREATE FUNCTION aqo_cleanup(OUT nfs integer, OUT nfss integer)
RETURNS record
AS 'MODULE_PATHNAME', 'aqo_cleanup'
LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION aqo_cleanup() IS
'Remove unneeded rows from the AQO ML storage';

CREATE FUNCTION aqo_disable_class(queryid bigint)
RETURNS void
AS 'MODULE_PATHNAME', 'aqo_disable_query'
LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION aqo_disable_class(bigint) IS
'Set learn_aqo, use_aqo and auto_tuning into false for a class of queries with specific queryid.';

--
-- Remove query class settings, text, statistics and ML data from AQO storage.
-- Return number of FSS records, removed from the storage.
--
CREATE FUNCTION aqo_drop_class(queryid bigint)
RETURNS integer
AS 'MODULE_PATHNAME', 'aqo_drop_class'
LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION aqo_drop_class(bigint) IS
'Remove info about an query class from AQO ML knowledge base.';

CREATE FUNCTION aqo_enable_class(queryid bigint)
RETURNS void
AS 'MODULE_PATHNAME', 'aqo_enable_query'
LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION aqo_enable_class(bigint) IS
'Set learn_aqo, use_aqo and auto_tuning (in intelligent mode) into true for a class of queries with specific queryid.';

--
-- Show execution time of queries, for which AQO has statistics.
-- controlled - show stat on executions where AQO was used for cardinality
-- estimations, or not used (controlled = false).
-- Last case is possible in disabled mode with aqo.force_collect_stat = 'on'.
--
CREATE FUNCTION aqo_execution_time(controlled boolean)
RETURNS TABLE(num integer, id bigint, fshash bigint, exec_time double precision, nexecs bigint)
AS 'MODULE_PATHNAME', 'aqo_execution_time'
LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION aqo_execution_time(boolean) IS
'Get execution time of queries. If controlled = true (AQO could advise cardinality estimations), show time of last execution attempt. Another case (AQO not used), return an average value of execution time across all known executions.';

CREATE FUNCTION aqo_memory_usage(
  OUT name text,
  OUT allocated_size int,
  OUT used_size int
)
RETURNS SETOF record
AS $$
  SELECT name, total_bytes, used_bytes FROM pg_backend_memory_contexts
  WHERE name LIKE 'AQO%'
  UNION
  SELECT name, allocated_size, size FROM pg_shmem_allocations
  WHERE name LIKE 'AQO%';
$$ LANGUAGE SQL;
COMMENT ON FUNCTION aqo_memory_usage() IS
'Show allocated sizes and used sizes of aqo`s memory contexts and hash tables';

--
-- Update or insert an aqo_data
-- table record for given 'fs' & 'fss'.
--

CREATE FUNCTION aqo_data_update(
  fs			bigint,
  fss			integer,
  nfeatures		integer,
  features		double precision[][],
  targets		double precision[],
  reliability	double precision[],
  oids			Oid[])
RETURNS bool
AS 'MODULE_PATHNAME', 'aqo_data_update'
LANGUAGE C VOLATILE;

CREATE FUNCTION aqo_queries_update(
  queryid bigint, fs bigint, learn_aqo bool, use_aqo bool, auto_tuning bool)
RETURNS bool
AS 'MODULE_PATHNAME', 'aqo_queries_update'
LANGUAGE C VOLATILE;

--
-- Update or insert an aqo_query_stat
-- table record for given 'queryid'.
--
CREATE FUNCTION aqo_query_stat_update(
  queryid						bigint,
  execution_time_with_aqo		double precision[],
  execution_time_without_aqo	double precision[],
  planning_time_with_aqo		double precision[],
  planning_time_without_aqo		double precision[],
  cardinality_error_with_aqo	double precision[],
  cardinality_error_without_aqo	double precision[],
  executions_with_aqo			bigint,
  executions_without_aqo		bigint)
RETURNS bool
AS 'MODULE_PATHNAME', 'aqo_query_stat_update'
LANGUAGE C VOLATILE;

--
-- Update or insert an aqo_query_texts
-- table record for given 'queryid'.
--
CREATE FUNCTION aqo_query_texts_update(
  queryid bigint, query_text text)
RETURNS bool
AS 'MODULE_PATHNAME', 'aqo_query_texts_update'
LANGUAGE C VOLATILE;

--
-- Remove all records in the AQO storage.
-- Return number of rows removed.
--
CREATE FUNCTION aqo_reset() RETURNS bigint
AS 'MODULE_PATHNAME', 'aqo_reset'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION aqo_reset() IS
'Reset all data gathered by AQO';

-- -----------------------------------------------------------------------------
--
-- VIEWs
--
-- -----------------------------------------------------------------------------

CREATE FUNCTION aqo_data (
  OUT fs			bigint,
  OUT fss			integer,
  OUT nfeatures		integer,
  OUT features		double precision[][],
  OUT targets		double precision[],
  OUT reliability	double precision[],
  OUT oids			Oid[]
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'aqo_data'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE FUNCTION aqo_queries (
  OUT queryid                bigint,
  OUT fs                     bigint,
  OUT learn_aqo              boolean,
  OUT use_aqo                boolean,
  OUT auto_tuning            boolean,
  OUT smart_timeout          bigint,
  OUT count_increase_timeout bigint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'aqo_queries'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE FUNCTION aqo_query_stat (
  OUT queryid						bigint,
  OUT execution_time_with_aqo		double precision[],
  OUT execution_time_without_aqo	double precision[],
  OUT planning_time_with_aqo		double precision[],
  OUT planning_time_without_aqo		double precision[],
  OUT cardinality_error_with_aqo	double precision[],
  OUT cardinality_error_without_aqo	double precision[],
  OUT executions_with_aqo bigint,
  OUT executions_without_aqo		bigint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'aqo_query_stat'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE FUNCTION aqo_query_texts(OUT queryid bigint, OUT query_text text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'aqo_query_texts'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE VIEW aqo_data AS SELECT * FROM aqo_data();
CREATE VIEW aqo_queries AS SELECT * FROM aqo_queries();
CREATE VIEW aqo_query_stat AS SELECT * FROM aqo_query_stat();
CREATE VIEW aqo_query_texts AS SELECT * FROM aqo_query_texts();
