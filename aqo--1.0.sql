-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION aqo" to load this file. \quit

CREATE TABLE aqo_queries (
	query_hash		int PRIMARY KEY,
	learn_aqo		boolean NOT NULL,
	use_aqo			boolean NOT NULL,
	fspace_hash		int NOT NULL,
	auto_tuning		boolean NOT NULL
);

CREATE TABLE aqo_query_texts (
	query_hash		int PRIMARY KEY REFERENCES aqo_queries ON DELETE CASCADE,
	query_text		varchar NOT NULL
);

CREATE TABLE aqo_query_stat (
	query_hash		int PRIMARY KEY REFERENCES aqo_queries ON DELETE CASCADE,
	execution_time_with_aqo					double precision[],
	execution_time_without_aqo				double precision[],
	planning_time_with_aqo					double precision[],
	planning_time_without_aqo				double precision[],
	cardinality_error_with_aqo				double precision[],
	cardinality_error_without_aqo			double precision[],
	executions_with_aqo						bigint,
	executions_without_aqo					bigint
);

CREATE TABLE aqo_data (
	fspace_hash		int NOT NULL REFERENCES aqo_queries ON DELETE CASCADE,
	fsspace_hash	int NOT NULL,
	nfeatures		int NOT NULL,
	features		double precision[][],
	targets			double precision[],
	UNIQUE (fspace_hash, fsspace_hash)
);

CREATE INDEX aqo_queries_query_hash_idx ON aqo_queries (query_hash);
CREATE INDEX aqo_query_texts_query_hash_idx ON aqo_query_texts (query_hash);
CREATE INDEX aqo_query_stat_idx ON aqo_query_stat (query_hash);
CREATE INDEX aqo_fss_access_idx ON aqo_data (fspace_hash, fsspace_hash);

ALTER TABLE aqo_data ALTER COLUMN features SET STORAGE MAIN;
ALTER TABLE aqo_data ALTER COLUMN targets SET STORAGE MAIN;
ALTER TABLE aqo_query_stat
ALTER COLUMN execution_time_with_aqo SET STORAGE MAIN;
ALTER TABLE aqo_query_stat
ALTER COLUMN execution_time_without_aqo SET STORAGE MAIN;
ALTER TABLE aqo_query_stat
ALTER COLUMN planning_time_with_aqo SET STORAGE MAIN;
ALTER TABLE aqo_query_stat
ALTER COLUMN planning_time_without_aqo SET STORAGE MAIN;
ALTER TABLE aqo_query_stat
ALTER COLUMN cardinality_error_without_aqo SET STORAGE MAIN;
ALTER TABLE aqo_query_stat
ALTER COLUMN cardinality_error_with_aqo SET STORAGE MAIN;

INSERT INTO aqo_queries VALUES (0, false, false, 0, false);
INSERT INTO aqo_query_texts VALUES (0, 'COMMON feature space (do not delete!)');
-- a virtual query for COMMON feature space

CREATE FUNCTION invalidate_deactivated_queries_cache() RETURNS trigger
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE TRIGGER aqo_queries_invalidate AFTER UPDATE OR DELETE OR TRUNCATE
	ON aqo_queries FOR EACH STATEMENT
	EXECUTE PROCEDURE invalidate_deactivated_queries_cache();
