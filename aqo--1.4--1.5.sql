/* contrib/aqo/aqo--1.4--1.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION aqo UPDATE TO '1.5'" to load this file. \quit

/* Remove old interface of the extension */
DROP FUNCTION array_mse;
DROP FUNCTION array_avg;
DROP FUNCTION public.aqo_clear_hist; -- Should be renamed and reworked
DROP FUNCTION public.aqo_disable_query;
DROP FUNCTION public.aqo_drop;
DROP FUNCTION public.aqo_enable_query;
DROP FUNCTION public.aqo_ne_queries; -- Not needed anymore due to changing in the logic
DROP FUNCTION public.aqo_status;
DROP FUNCTION public.clean_aqo_data;
DROP FUNCTION public.show_cardinality_errors;
DROP FUNCTION public.top_time_queries;
DROP TABLE public.aqo_data CASCADE;
DROP TABLE public.aqo_queries CASCADE;
DROP TABLE public.aqo_query_texts CASCADE;
DROP TABLE public.aqo_query_stat CASCADE;


/*
 * VIEWs to discover AQO data.
 */
CREATE FUNCTION aqo_queries (
  OUT queryid		bigint,
  OUT fs			bigint,
  OUT learn_aqo		boolean,
  OUT use_aqo		boolean,
  OUT auto_tuning	boolean
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'aqo_queries'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE FUNCTION aqo_query_texts(OUT queryid bigint, OUT query_text text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'aqo_query_texts'
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

CREATE FUNCTION aqo_data (
  OUT fs			bigint,
  OUT fss			integer,
  OUT nfeatures		integer,
  OUT features		double precision[][],
  OUT targets		double precision[],
  OUT reliability	double precision[],
  OUT oids			integer[]
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'aqo_data'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE VIEW aqo_query_stat AS SELECT * FROM aqo_query_stat();
CREATE VIEW aqo_query_texts AS SELECT * FROM aqo_query_texts();
CREATE VIEW aqo_data AS SELECT * FROM aqo_data();
CREATE VIEW aqo_queries AS SELECT * FROM aqo_queries();

/* UI functions */


CREATE FUNCTION aqo_enable_query(queryid bigint)
RETURNS void
AS 'MODULE_PATHNAME', 'aqo_enable_query'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION aqo_disable_query(queryid bigint)
RETURNS void
AS 'MODULE_PATHNAME', 'aqo_enable_query'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION aqo_queries_update(
  queryid bigint, fs bigint, learn_aqo bool, use_aqo bool, auto_tuning bool)
RETURNS bool
AS 'MODULE_PATHNAME', 'aqo_queries_update'
LANGUAGE C VOLATILE;

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
CREATE OR REPLACE FUNCTION aqo_cardinality_error(controlled boolean)
RETURNS TABLE(num bigint, id bigint, fshash bigint, error float, nexecs bigint)
AS 'MODULE_PATHNAME', 'aqo_cardinality_error'
LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION aqo_cardinality_error(boolean) IS
'Get cardinality error of queries the last time they were executed. Order queries according to an error value.';

--
-- Show execution time of queries, for which AQO has statistics.
-- controlled - show stat on executions where AQO was used for cardinality
-- estimations, or not used (controlled = false).
-- Last case is possible in disabled mode with aqo.force_collect_stat = 'on'.
--
CREATE OR REPLACE FUNCTION aqo_execution_time(controlled boolean)
RETURNS TABLE(num bigint, id bigint, fshash bigint, exec_time float, nexecs bigint)
AS 'MODULE_PATHNAME', 'aqo_execution_time'
LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION aqo_execution_time(boolean) IS
'Get execution time of queries. If controlled = true (AQO could advise cardinality estimations), show time of last execution attempt. Another case (AQO not used), return an average value of execution time across all known executions.';

--
-- Remove query class settings, text, statistics and ML data from AQO storage.
-- Return number of FSS records, removed from the storage.
--
CREATE OR REPLACE FUNCTION aqo_drop_class(queryid bigint)
RETURNS integer
AS 'MODULE_PATHNAME', 'aqo_drop_class'
LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION aqo_drop_class(bigint) IS
'Remove info about an query class from AQO ML knowledge base.';

--
-- Remove unneeded rows from the AQO ML storage.
-- For common feature space, remove rows from aqo_data only.
-- For custom feature space - remove all rows related to the space from all AQO
-- tables even if only one oid for one feature subspace of the space is illegal.
-- Returns number of deleted rows from aqo_queries and aqo_data tables.
--
CREATE OR REPLACE FUNCTION aqo_cleanup(OUT nfs integer, OUT nfss integer)
AS 'MODULE_PATHNAME', 'aqo_cleanup'
LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION aqo_cleanup() IS
'Remove unneeded rows from the AQO ML storage';

--
-- Remove all records in the AQO storage.
-- Return number of rows removed.
--
CREATE FUNCTION aqo_reset() RETURNS bigint
AS 'MODULE_PATHNAME', 'aqo_reset'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION aqo_reset() IS
'Reset all data gathered by AQO';
