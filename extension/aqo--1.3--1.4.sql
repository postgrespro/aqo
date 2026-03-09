/* contrib/aqo/aqo--1.3--1.4.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION aqo UPDATE TO '1.4'" to load this file. \quit

ALTER TABLE public.aqo_data ADD COLUMN reliability double precision [];

DROP FUNCTION public.top_error_queries(int);

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
CREATE OR REPLACE FUNCTION public.show_cardinality_errors(controlled boolean)
RETURNS TABLE(num bigint, id bigint, fshash bigint, error float, nexecs bigint)
AS $$
BEGIN
IF (controlled) THEN
  RETURN QUERY
    SELECT
      row_number() OVER (ORDER BY (cerror, query_id, fs_hash) DESC) AS nn,
      query_id, fs_hash, cerror, execs
    FROM (
    SELECT
      aq.query_hash AS query_id,
      aq.fspace_hash AS fs_hash,
      cardinality_error_with_aqo[array_length(cardinality_error_with_aqo, 1)] AS cerror,
      executions_with_aqo AS execs
    FROM public.aqo_queries aq JOIN public.aqo_query_stat aqs
    ON aq.query_hash = aqs.query_hash
    WHERE TRUE = ANY (SELECT unnest(cardinality_error_with_aqo) IS NOT NULL)
    ) AS q1
    ORDER BY nn ASC;
ELSE
  RETURN QUERY
    SELECT
      row_number() OVER (ORDER BY (cerror, query_id, fs_hash) DESC) AS nn,
      query_id, fs_hash, cerror, execs
    FROM (
      SELECT
        aq.query_hash AS query_id,
        aq.fspace_hash AS fs_hash,
        array_avg(cardinality_error_without_aqo) AS cerror,
        executions_without_aqo AS execs
      FROM public.aqo_queries aq JOIN public.aqo_query_stat aqs
      ON aq.query_hash = aqs.query_hash
      WHERE TRUE = ANY (SELECT unnest(cardinality_error_without_aqo) IS NOT NULL)
      ) AS q1
    ORDER BY (nn) ASC;
END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.show_cardinality_errors(boolean) IS
'Get cardinality error of queries the last time they were executed. Order queries according to an error value.';
