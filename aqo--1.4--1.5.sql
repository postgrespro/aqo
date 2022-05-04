/* contrib/aqo/aqo--1.4--1.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION aqo UPDATE TO '1.5'" to load this file. \quit

--
-- Re-create the aqo_data table. Do so to keep the columns order.
--
DROP TABLE public.aqo_data CASCADE;
CREATE TABLE public.aqo_data (
	fspace_hash		bigint NOT NULL REFERENCES public.aqo_queries ON DELETE CASCADE,
	fsspace_hash	int NOT NULL,
	nfeatures		int NOT NULL,
	features		double precision[][],
	targets			double precision[],
	oids			oid [] DEFAULT NULL,
	reliability		double precision []
);
CREATE UNIQUE INDEX aqo_fss_access_idx ON public.aqo_data (fspace_hash, fsspace_hash);


--
-- Remove rows from the AQO ML knowledge base, related to previously dropped
-- tables of the database.
--
CREATE OR REPLACE FUNCTION public.clean_aqo_data() RETURNS void AS $$
DECLARE
    aqo_data_row aqo_data%ROWTYPE;
    aqo_queries_row aqo_queries%ROWTYPE;
    aqo_query_texts_row aqo_query_texts%ROWTYPE;
    aqo_query_stat_row aqo_query_stat%ROWTYPE;
    oid_var oid;
    fspace_hash_var bigint;
    delete_row boolean DEFAULT false;
BEGIN
  FOR aqo_data_row IN (SELECT * FROM aqo_data)
  LOOP
    delete_row = false;
    SELECT aqo_data_row.fspace_hash INTO fspace_hash_var FROM aqo_data;

    IF (aqo_data_row.oids IS NOT NULL) THEN
      FOREACH oid_var IN ARRAY aqo_data_row.oids
      LOOP
        IF NOT EXISTS (SELECT relname FROM pg_class WHERE oid = oid_var) THEN
          delete_row = true;
        END IF;
      END LOOP;
    END IF;

    FOR aqo_queries_row IN (SELECT * FROM public.aqo_queries)
    LOOP
      IF (delete_row = true AND fspace_hash_var <> 0 AND
          fspace_hash_var = aqo_queries_row.fspace_hash AND
          aqo_queries_row.fspace_hash = aqo_queries_row.query_hash) THEN
        DELETE FROM aqo_data WHERE aqo_data = aqo_data_row;
        DELETE FROM aqo_queries WHERE aqo_queries = aqo_queries_row;

        FOR aqo_query_texts_row IN (SELECT * FROM aqo_query_texts)
        LOOP
          DELETE FROM aqo_query_texts
          WHERE aqo_query_texts_row.query_hash = fspace_hash_var AND
				aqo_query_texts = aqo_query_texts_row;
        END LOOP;

        FOR aqo_query_stat_row IN (SELECT * FROM aqo_query_stat)
        LOOP
          DELETE FROM aqo_query_stat
          WHERE aqo_query_stat_row.query_hash = fspace_hash_var AND
				aqo_query_stat = aqo_query_stat_row;
        END LOOP;
      END IF;
    END LOOP;
  END LOOP;
END;
$$ LANGUAGE plpgsql;