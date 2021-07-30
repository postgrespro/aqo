CREATE EXTENSION aqo;
SET aqo.mode = 'learn';

CREATE TABLE a();
SELECT * FROM a;
SELECT 'a'::regclass::oid AS a_oid \gset
SELECT clean_aqo_data();
-- lines with a_oid should remain
SELECT count(*) FROM aqo_data WHERE :a_oid=ANY(oids);

DROP TABLE a;
SELECT clean_aqo_data();
--  lines with a_oid should be deleted
SELECT count(*) FROM aqo_data WHERE :a_oid=ANY(oids);

CREATE TABLE a();
CREATE TABLE b();
SELECT * FROM a;
SELECT * FROM b;
SELECT * FROM b CROSS JOIN a;
SELECT 'a'::regclass::oid AS a_oid \gset
SELECT 'b'::regclass::oid AS b_oid \gset
-- new lines added to aqo_data
SELECT count(*) FROM aqo_data WHERE :a_oid=ANY(oids);
SELECT count(*) FROM aqo_data WHERE :b_oid=ANY(oids);
DROP TABLE a;
SELECT clean_aqo_data();
-- lines with a_oid deleted, including line with both a_oid and b_oid
SELECT count(*) FROM aqo_data WHERE :a_oid=ANY(oids);
SELECT count(*) FROM aqo_data WHERE :b_oid=ANY(oids);
DROP TABLE b;
SELECT clean_aqo_data();
-- lines with b_oid deleted
SELECT count(*) FROM aqo_data WHERE :b_oid=ANY(oids);