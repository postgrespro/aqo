-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION aqo" to load this file. \quit

CREATE TABLE public.aqo_queries (
	query_hash		bigint CONSTRAINT aqo_queries_query_hash_idx PRIMARY KEY,
	learn_aqo		boolean NOT NULL,
	use_aqo			boolean NOT NULL,
	fspace_hash		bigint NOT NULL,
	auto_tuning		boolean NOT NULL
);

CREATE TABLE public.aqo_query_texts (
	query_hash		bigint CONSTRAINT aqo_query_texts_query_hash_idx PRIMARY KEY REFERENCES public.aqo_queries ON DELETE CASCADE,
	query_text		text NOT NULL
);

CREATE TABLE public.aqo_query_stat (
	query_hash		bigint CONSTRAINT aqo_query_stat_idx PRIMARY KEY REFERENCES public.aqo_queries ON DELETE CASCADE,
	execution_time_with_aqo					double precision[],
	execution_time_without_aqo				double precision[],
	planning_time_with_aqo					double precision[],
	planning_time_without_aqo				double precision[],
	cardinality_error_with_aqo				double precision[],
	cardinality_error_without_aqo			double precision[],
	executions_with_aqo						bigint,
	executions_without_aqo					bigint
);

CREATE TABLE public.aqo_data (
	fspace_hash		bigint NOT NULL REFERENCES public.aqo_queries ON DELETE CASCADE,
	fsspace_hash	int NOT NULL,
	nfeatures		int NOT NULL,
	features		double precision[][],
	targets			double precision[]
);

CREATE UNIQUE INDEX aqo_fss_access_idx ON public.aqo_data (fspace_hash, fsspace_hash);

INSERT INTO public.aqo_queries VALUES (0, false, false, 0, false);
INSERT INTO public.aqo_query_texts VALUES (0, 'COMMON feature space (do not delete!)');
-- a virtual query for COMMON feature space

CREATE FUNCTION invalidate_deactivated_queries_cache() RETURNS trigger
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE TRIGGER aqo_queries_invalidate AFTER UPDATE OR DELETE OR TRUNCATE
	ON public.aqo_queries FOR EACH STATEMENT
	EXECUTE PROCEDURE invalidate_deactivated_queries_cache();

--
-- Service functions
--

-- Show query state at the AQO knowledge base
CREATE FUNCTION public.aqo_status(hash bigint)
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

CREATE FUNCTION public.aqo_enable_query(hash bigint)
RETURNS VOID
AS $func$
UPDATE public.aqo_queries SET
	learn_aqo = 'true',
	use_aqo = 'true'
	WHERE query_hash = $1;
$func$ LANGUAGE SQL;

CREATE FUNCTION public.aqo_disable_query(hash bigint)
RETURNS VOID
AS $func$
UPDATE public.aqo_queries SET
	learn_aqo = 'false',
	use_aqo = 'false',
	auto_tuning = 'false'
	WHERE query_hash = $1;
$func$ LANGUAGE SQL;

CREATE FUNCTION public.aqo_clear_hist(hash bigint)
RETURNS VOID
AS $func$
DELETE FROM public.aqo_data WHERE fspace_hash=$1;
$func$ LANGUAGE SQL;

-- Show queries that contains 'Never executed' nodes at the plan.
CREATE FUNCTION public.aqo_ne_queries()
RETURNS SETOF int
AS $func$
SELECT query_hash FROM public.aqo_query_stat aqs
	WHERE -1 = ANY (cardinality_error_with_aqo::double precision[]);
$func$ LANGUAGE SQL;

CREATE FUNCTION public.aqo_drop(hash bigint)
RETURNS VOID
AS $func$
DELETE FROM public.aqo_queries aq WHERE (aq.query_hash = $1);
DELETE FROM public.aqo_data ad WHERE (ad.fspace_hash = $1);
DELETE FROM public.aqo_query_stat aq WHERE (aq.query_hash = $1);
DELETE FROM public.aqo_query_texts aq WHERE (aq.query_hash = $1);
$func$ LANGUAGE SQL;
