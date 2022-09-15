/* contrib/aqo/aqo--1.3--1.4.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION aqo UPDATE TO '1.4'" to load this file. \quit

ALTER TABLE public.aqo_data ADD COLUMN reliability double precision [];

--
-- Get IDs of queries having the largest cardinality error when last executed.
-- num - sequental number. Smaller number corresponds to higher error.
-- qhash - ID of a query.
-- error - AQO error calculated over plan nodes of the query.
--
CREATE OR REPLACE FUNCTION public.show_cardinality_errors()
RETURNS TABLE(num bigint, id bigint, error float)
AS $$
BEGIN
  RETURN QUERY
	 SELECT
	  row_number() OVER (ORDER BY (cerror, qhash) DESC) AS nn,
	  qhash, cerror
	FROM (
	SELECT
	  aq.query_hash AS qhash,
	  cardinality_error_with_aqo[array_length(cardinality_error_with_aqo, 1)] AS cerror
	FROM public.aqo_queries aq JOIN public.aqo_query_stat aqs
	ON aq.query_hash = aqs.query_hash
	WHERE TRUE = ANY (SELECT unnest(cardinality_error_with_aqo) IS NOT NULL)
	) AS q1
	ORDER BY nn ASC;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.show_cardinality_errors() IS
'Get cardinality error of last query execution. Return queries having the largest error.';
