SELECT INSTR('foobarbar', 'bar');
SELECT INSTR('bar', 'foobar');
SELECT INSTR('xbar', NULL);
SELECT INSTR(NULL, 'foobar');
CREATE TABLE table_1(val1 UInt64, val2 String,val3 String) ENGINE=CnchMergeTree() ORDER BY val1;
INSERT INTO table_1 VALUES(0, 'foobarbar', 'bar');
INSERT INTO table_1 VALUES(1, 'ar', 'bar');
INSERT INTO table_1 VALUES(2, 'foobarbar', 'x');
SELECT val1, INSTR(val2, val3) FROM table_1 ORDER BY val1 ASC;
