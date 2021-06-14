\set citizens	1000

SET aqo.mode = 'disabled';
SET aqo.force_collect_stat = 'on';

CREATE TABLE person (
    id serial PRIMARY KEY,
    age integer,
    gender text,
	passport integer
);

-- Fill the person table with workers data.
INSERT INTO person (id,age,gender,passport)
	(SELECT q1.id,q1.age,
	 		CASE WHEN q1.id % 4 = 0 THEN 'Female'
	 			 ELSE 'Male'
	 		END,
	 		CASE WHEN (q1.age>18) THEN 1E6 + q1.id * 1E3
	 			 ELSE NULL
	 		END
	 FROM (SELECT *, 14+(id % 60) AS age FROM generate_series(1, :citizens) id) AS q1
	);

CREATE EXTENSION aqo;

SELECT count(*) FROM person WHERE age<18;
SELECT count(*) FROM person WHERE age<18 AND passport IS NOT NULL;
SELECT * FROM aqo_data;

SELECT learn_aqo,use_aqo,auto_tuning,cardinality_error_without_aqo ce,executions_without_aqo nex
FROM aqo_queries JOIN aqo_query_stat USING (query_hash);

SELECT query_text FROM aqo_query_texts ORDER BY (query_text);

DROP EXTENSION aqo;
