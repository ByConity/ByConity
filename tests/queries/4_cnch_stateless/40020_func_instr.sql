SELECT INSTR('foobarbar', 'bar');
SELECT LOCATE('foobarbar', 'bar'); -- clickhouse locate

set enable_optimizer=1;
set dialect_type='MYSQL';
SELECT INSTR('foobarbar', 'bar'); -- mysql instr
SELECT LOCATE('bar', 'foobarbar'); -- mysql locate

SELECT INSTR('bar', 'foobar');
SELECT INSTR('xbar', NULL);
SELECT INSTR(NULL, 'foobar');

DROP TABLE IF EXISTS table_instr_0;
CREATE TABLE table_instr_0(val1 UInt64, val2 String,val3 String) ENGINE=CnchMergeTree() ORDER BY val1;
INSERT INTO table_instr_0 VALUES(0, 'foobarbar', 'bar');
INSERT INTO table_instr_0 VALUES(1, 'ar', 'bar');
INSERT INTO table_instr_0 VALUES(2, 'foobarbar', 'x');
SELECT val1, INSTR(val2, val3) FROM table_instr_0 ORDER BY val1 ASC;
DROP TABLE table_instr_0;
