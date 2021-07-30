ALTER TABLE public.aqo_data ADD COLUMN oids OID [] DEFAULT NULL;

CREATE OR REPLACE FUNCTION public.clean_aqo_data() RETURNS void AS $$
DECLARE
    aqo_data_row aqo_data%ROWTYPE;
    oid_var oid;
    delete_row boolean DEFAULT false;
BEGIN
    RAISE NOTICE 'Cleaning aqo_data records';

    FOR aqo_data_row IN 
        SELECT * FROM aqo_data
    LOOP 
        delete_row = false;
        FOREACH oid_var IN ARRAY aqo_data_row.oids
        LOOP
            IF NOT EXISTS (SELECT relname FROM pg_class WHERE oid = oid_var) THEN 
                delete_row = true;
            END IF;
        END LOOP;
        IF delete_row = true THEN
            DELETE FROM aqo_data WHERE aqo_data = aqo_data_row;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;