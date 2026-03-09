/* contrib/aqo/aqo--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION aqo UPDATE TO '1.6'" to load this file. \quit

DROP VIEW aqo_queries;

DROP FUNCTION aqo_enable_query;
DROP FUNCTION aqo_disable_query;
DROP FUNCTION aqo_cleanup;
DROP FUNCTION aqo_queries;

CREATE FUNCTION aqo_enable_class(queryid bigint)
RETURNS void
AS 'MODULE_PATHNAME', 'aqo_enable_query'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION aqo_disable_class(queryid bigint)
RETURNS void
AS 'MODULE_PATHNAME', 'aqo_disable_query'
LANGUAGE C STRICT VOLATILE;

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
-- Update or insert an aqo_query_stat
-- table record for given 'queryid'.
--

CREATE FUNCTION aqo_query_stat_update(
  queryid			bigint,
  execution_time_with_aqo	double precision[],
  execution_time_without_aqo	double precision[],
  planning_time_with_aqo	double precision[],
  planning_time_without_aqo	double precision[],
  cardinality_error_with_aqo	double precision[],
  cardinality_error_without_aqo	double precision[],
  executions_with_aqo		bigint,
  executions_without_aqo	bigint)
RETURNS bool
AS 'MODULE_PATHNAME', 'aqo_query_stat_update'
LANGUAGE C VOLATILE;

--
-- Update or insert an aqo_data
-- table record for given 'fs' & 'fss'.
--

CREATE FUNCTION aqo_data_update(
  fs		bigint,
  fss		integer,
  nfeatures	integer,
  features	double precision[][],
  targets	double precision[],
  reliability	double precision[],
  oids		Oid[])
RETURNS bool
AS 'MODULE_PATHNAME', 'aqo_data_update'
LANGUAGE C VOLATILE;

/*
 * VIEWs to discover AQO data.
 */
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

CREATE VIEW aqo_queries AS SELECT * FROM aqo_queries();

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
