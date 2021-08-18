ALTER TABLE public.aqo_data ADD COLUMN oids OID [] DEFAULT NULL;

CREATE OR REPLACE FUNCTION public.clean_aqo_data() RETURNS void AS $$
DECLARE
    aqo_data_row aqo_data%ROWTYPE;
    aqo_queries_row aqo_queries%ROWTYPE;
    aqo_query_texts_row aqo_query_texts%ROWTYPE;
    aqo_query_stat_row aqo_query_stat%ROWTYPE;
    oid_var oid;
    fspace_hash_var int;
    delete_row boolean DEFAULT false;
BEGIN
    RAISE NOTICE 'Cleaning aqo_data records';
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
        FOR aqo_queries_row IN (SELECT * FROM aqo_queries)
        LOOP
            IF (delete_row = true AND
                fspace_hash_var <> 0 AND
                fspace_hash_var = aqo_queries_row.fspace_hash AND
                aqo_queries_row.fspace_hash = aqo_queries_row.query_hash
               ) THEN
                DELETE FROM aqo_data WHERE aqo_data = aqo_data_row;
                DELETE FROM aqo_queries WHERE aqo_queries = aqo_queries_row;
                FOR aqo_query_texts_row IN (SELECT * FROM aqo_query_texts)
                LOOP
                    DELETE FROM aqo_query_texts WHERE aqo_query_texts_row.query_hash = fspace_hash_var AND
                                                aqo_query_texts = aqo_query_texts_row;
                END LOOP;
                FOR aqo_query_stat_row IN (SELECT * FROM aqo_query_stat)
                LOOP
                    DELETE FROM aqo_query_stat WHERE aqo_query_stat_row.query_hash = fspace_hash_var AND
                                                aqo_query_stat = aqo_query_stat_row;
                END LOOP;
            END IF;
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION array_avg(arr double precision[]) RETURNS double precision as $$
BEGIN
    RETURN (SELECT AVG(a) FROM UNNEST(arr) AS a);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION array_mse(arr double precision[]) RETURNS double precision as $$
DECLARE
    mean double precision;
BEGIN
    mean = array_avg(arr);
    RETURN (SELECT AVG(POWER(a - mean, 2)) FROM UNNEST(arr) AS a);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.top_time_queries(n int)
    RETURNS TABLE(num bigint,
                  fspace_hash int,
                  query_hash int,
                  execution_time double precision[],
                  mean double precision,
                  mean_square_error double precision
                 )
AS $$
BEGIN
    RAISE NOTICE 'Top % execution time queries', n;
    RETURN QUERY SELECT row_number() OVER(ORDER BY execution_time_without_aqo DESC) num,
                        aqo_queries.fspace_hash,
                        aqo_queries.query_hash,
                        execution_time_without_aqo,
                        array_avg(execution_time_without_aqo) AS average,
                        array_mse(execution_time_without_aqo)
        FROM aqo_queries INNER JOIN aqo_query_stat ON aqo_queries.query_hash = aqo_query_stat.query_hash
            GROUP BY execution_time_without_aqo, aqo_queries.fspace_hash, aqo_queries.query_hash
                ORDER BY average DESC LIMIT n;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.top_error_queries(n int)
    RETURNS TABLE(num bigint,
                  fspace_hash int,
                  query_hash int,
                  cardinality_error double precision[],
                  mean double precision,
                  mean_square_error double precision
                 )
AS $$
BEGIN
    RAISE NOTICE 'Top % cardinality error queries', n;
    RETURN QUERY SELECT row_number() OVER(ORDER BY cardinality_error_without_aqo DESC) num,
                        aqo_queries.fspace_hash,
                        aqo_queries.query_hash,
                        cardinality_error_without_aqo,
                        array_avg(cardinality_error_without_aqo) AS average,
                        array_mse(cardinality_error_without_aqo)
        FROM aqo_queries INNER JOIN aqo_query_stat ON aqo_queries.query_hash = aqo_query_stat.query_hash
            GROUP BY cardinality_error_without_aqo, aqo_queries.fspace_hash, aqo_queries.query_hash
                ORDER BY average DESC LIMIT n;
END;
$$ LANGUAGE plpgsql;