ALTER TABLE public.aqo_query_texts ALTER COLUMN query_text TYPE text;


DROP INDEX public.aqo_queries_query_hash_idx CASCADE;
DROP INDEX public.aqo_query_texts_query_hash_idx CASCADE;
DROP INDEX public.aqo_query_stat_idx CASCADE;
DROP INDEX public.aqo_fss_access_idx CASCADE;

CREATE UNIQUE INDEX aqo_fss_access_idx
	ON public.aqo_data (fspace_hash, fsspace_hash);


CREATE OR REPLACE FUNCTION aqo_migrate_to_1_1_get_pk(rel regclass) RETURNS regclass AS $$
DECLARE
	idx regclass;
BEGIN
	SELECT i.indexrelid FROM pg_catalog.pg_index i JOIN
	pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND
								 a.attnum = ANY(i.indkey)
	WHERE i.indrelid = rel AND i.indisprimary
	INTO idx;

	RETURN idx;
END
$$ LANGUAGE plpgsql;


DO $$
BEGIN
	EXECUTE pg_catalog.format('ALTER TABLE %s RENAME to %s',
				   aqo_migrate_to_1_1_get_pk('public.aqo_queries'),
				   'aqo_queries_query_hash_idx');

	EXECUTE pg_catalog.format('ALTER TABLE %s RENAME to %s',
				   aqo_migrate_to_1_1_get_pk('public.aqo_query_texts'),
				   'aqo_query_texts_query_hash_idx');

	EXECUTE pg_catalog.format('ALTER TABLE %s RENAME to %s',
				   aqo_migrate_to_1_1_get_pk('public.aqo_query_stat'),
				   'aqo_query_stat_idx');
END
$$;

DROP FUNCTION aqo_migrate_to_1_1_get_pk(regclass);
