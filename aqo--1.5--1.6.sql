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

/*
 * VIEWs to discover AQO data.
 */
CREATE FUNCTION aqo_queries (
  OUT queryid		bigint,
  OUT fs			bigint,
  OUT learn_aqo		boolean,
  OUT use_aqo		boolean,
  OUT auto_tuning	boolean,
  OUT smart_timeout bigint,
  OUT count_increase_timeout bigint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'aqo_queries'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE VIEW aqo_queries AS SELECT * FROM aqo_queries();
