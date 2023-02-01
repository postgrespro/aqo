CREATE EXTENSION IF NOT EXISTS aqo;
SELECT true AS success FROM aqo_reset();

\set citizens	1000

SET aqo.mode = 'disabled';
SET aqo.force_collect_stat = 'off';

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

SET aqo.force_collect_stat = 'on';

SELECT count(*) FROM person WHERE age<18;
SELECT count(*) FROM person WHERE age<18 AND passport IS NOT NULL;
SELECT * FROM aqo_data;

CREATE OR REPLACE FUNCTION round_array (double precision[])
RETURNS double precision[]
LANGUAGE SQL
AS $$
   SELECT array_agg(round(elem::numeric, 3))
   FROM unnest($1) as arr(elem);
$$;

SELECT learn_aqo,use_aqo,auto_tuning,round_array(cardinality_error_without_aqo) ce,executions_without_aqo nex
FROM aqo_queries AS aq JOIN aqo_query_stat AS aqs
ON aq.queryid = aqs.queryid
ORDER BY (cardinality_error_without_aqo);

SELECT query_text FROM aqo_query_texts ORDER BY (md5(query_text));

DROP TABLE person;

DROP EXTENSION aqo;
