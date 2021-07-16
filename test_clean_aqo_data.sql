CREATE TABLE a();
SELECT * FROM a;
SELECT oids FROM aqo_data;
SELECT clean_aqo_data();
-- в aqo_data не должна пропасть строка с oid a
SELECT oids FROM aqo_data;

DROP TABLE a;
SELECT clean_aqo_data();
-- в aqo_data должна исчезнуть запись
SELECT oids FROM aqo_data;

CREATE TABLE a();
CREATE TABLE b();
SELECT * FROM a;
SELECT * FROM b;
SELECT * FROM b CROSS JOIN a;
-- в aqo_data появились 3 записи
SELECT oids FROM aqo_data;
DROP TABLE a;
SELECT clean_aqo_data();
-- в aqo_data исчезли 2 записи
SELECT oids FROM aqo_data;