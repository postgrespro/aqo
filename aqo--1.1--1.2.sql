CREATE OR REPLACE FUNCTION aqo_migrate_to_1_2_get_pk(relid regclass) RETURNS text AS $$
DECLARE
	res text;
BEGIN
	SELECT conname
		FROM pg_catalog.pg_constraint
		WHERE conrelid = relid AND contype = 'u'
	INTO res;

	RETURN res;
END
$$ LANGUAGE plpgsql;

DO $$
BEGIN
	EXECUTE pg_catalog.format(
					'ALTER TABLE public.aqo_data DROP CONSTRAINT %s',
					aqo_migrate_to_1_2_get_pk('public.aqo_data'::regclass),
					'aqo_queries_query_hash_idx');
END
$$;


DROP FUNCTION aqo_migrate_to_1_2_get_pk(regclass);

--
-- Service functions
--

-- Show query state at the AQO knowledge base
CREATE OR REPLACE FUNCTION public.aqo_status(hash bigint)
RETURNS TABLE (
	"learn"			BOOL,
	"use aqo"		BOOL,
	"auto tune"		BOOL,
	"fspace hash"	bigINT,
	"t_naqo"		TEXT,
	"err_naqo"		TEXT,
	"iters"			BIGINT,
	"t_aqo"			TEXT,
	"err_aqo"		TEXT,
	"iters_aqo"		BIGINT
) 
AS $func$
SELECT	learn_aqo,use_aqo,auto_tuning,fspace_hash,
		to_char(execution_time_without_aqo[n4],'9.99EEEE'),
		to_char(cardinality_error_without_aqo[n2],'9.99EEEE'),
		executions_without_aqo,
		to_char(execution_time_with_aqo[n3],'9.99EEEE'),
		to_char(cardinality_error_with_aqo[n1],'9.99EEEE'),
		executions_with_aqo
FROM public.aqo_queries aq, public.aqo_query_stat aqs,
	(SELECT array_length(n1,1) AS n1, array_length(n2,1) AS n2,
		array_length(n3,1) AS n3, array_length(n4,1) AS n4
	FROM
		(SELECT cardinality_error_with_aqo		AS n1,
				cardinality_error_without_aqo	AS n2,
				execution_time_with_aqo			AS n3,
				execution_time_without_aqo		AS n4
		FROM public.aqo_query_stat aqs WHERE
			aqs.query_hash = $1) AS al) AS q
WHERE (aqs.query_hash = aq.query_hash) AND
	aqs.query_hash = $1;
$func$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION public.aqo_enable_query(hash bigint)
RETURNS VOID
AS $func$
UPDATE public.aqo_queries SET
	learn_aqo = 'true',
	use_aqo = 'true'
	WHERE query_hash = $1;
$func$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION public.aqo_disable_query(hash bigint)
RETURNS VOID
AS $func$
UPDATE public.aqo_queries SET
	learn_aqo = 'false',
	use_aqo = 'false',
	auto_tuning = 'false'
	WHERE query_hash = $1;
$func$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION public.aqo_clear_hist(hash bigint)
RETURNS VOID
AS $func$
DELETE FROM public.aqo_data WHERE fspace_hash=$1;
$func$ LANGUAGE SQL;

-- Show queries that contains 'Never executed' nodes at the plan.
CREATE OR REPLACE FUNCTION public.aqo_ne_queries()
RETURNS SETOF int
AS $func$
SELECT query_hash FROM public.aqo_query_stat aqs
	WHERE -1 = ANY (cardinality_error_with_aqo::double precision[]);
$func$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION public.aqo_drop(hash bigint)
RETURNS VOID
AS $func$
DELETE FROM public.aqo_queries aq WHERE (aq.query_hash = $1);
DELETE FROM public.aqo_data ad WHERE (ad.fspace_hash = $1);
DELETE FROM public.aqo_query_stat aq WHERE (aq.query_hash = $1);
DELETE FROM public.aqo_query_texts aq WHERE (aq.query_hash = $1);
$func$ LANGUAGE SQL;
