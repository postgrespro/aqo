-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION aqo" to load this file. \quit

CREATE TABLE aqo_queries (
	query_hash		int4 CONSTRAINT "aqo_queries_query_hash_idx"
						 PRIMARY KEY,
	learn_aqo		bool	NOT NULL,
	use_aqo			bool	NOT NULL,
	fspace_hash		int4	NOT NULL,
	auto_tuning		bool	NOT NULL
);

CREATE TABLE aqo_query_texts (
	query_hash		int4 CONSTRAINT "aqo_query_texts_query_hash_idx"
					     PRIMARY KEY REFERENCES aqo_queries ON DELETE CASCADE,
	query_text		text	NOT NULL
);

CREATE TABLE aqo_query_stat (
	query_hash		int4 CONSTRAINT "aqo_query_stat_idx"
						 PRIMARY KEY REFERENCES aqo_queries ON DELETE CASCADE,
	execution_time_with_aqo			float8[],
	execution_time_without_aqo		float8[],
	planning_time_with_aqo			float8[],
	planning_time_without_aqo		float8[],
	cardinality_error_with_aqo		float8[],
	cardinality_error_without_aqo	float8[],
	executions_with_aqo				int8,
	executions_without_aqo			int8
);

CREATE TABLE aqo_data (
	fspace_hash		int4 NOT NULL REFERENCES aqo_queries ON DELETE CASCADE,
	fsspace_hash	int4 NOT NULL,
	nfeatures		int4 NOT NULL,
	features		float8[][],
	targets			float8[]
);

CREATE UNIQUE INDEX aqo_fss_access_idx ON aqo_data (fspace_hash, fsspace_hash);

ALTER TABLE aqo_data		ALTER COLUMN features	SET STORAGE MAIN;
ALTER TABLE aqo_data		ALTER COLUMN targets	SET STORAGE MAIN;
ALTER TABLE aqo_query_stat	ALTER COLUMN execution_time_with_aqo		SET STORAGE MAIN;
ALTER TABLE aqo_query_stat	ALTER COLUMN execution_time_without_aqo		SET STORAGE MAIN;
ALTER TABLE aqo_query_stat	ALTER COLUMN planning_time_with_aqo			SET STORAGE MAIN;
ALTER TABLE aqo_query_stat	ALTER COLUMN planning_time_without_aqo		SET STORAGE MAIN;
ALTER TABLE aqo_query_stat	ALTER COLUMN cardinality_error_without_aqo	SET STORAGE MAIN;
ALTER TABLE aqo_query_stat	ALTER COLUMN cardinality_error_with_aqo		SET STORAGE MAIN;

INSERT INTO aqo_queries VALUES (0, false, false, 0, false);
INSERT INTO aqo_query_texts VALUES (0, 'COMMON feature space (do not delete!)');
-- a virtual query for COMMON feature space

CREATE FUNCTION invalidate_deactivated_queries_cache() RETURNS trigger
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE TRIGGER aqo_queries_invalidate AFTER UPDATE OR DELETE OR TRUNCATE
	ON aqo_queries FOR EACH STATEMENT
	EXECUTE PROCEDURE invalidate_deactivated_queries_cache();
