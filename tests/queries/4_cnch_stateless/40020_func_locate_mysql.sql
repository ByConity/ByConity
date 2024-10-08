set enable_optimizer=1;
set dialect_type='MYSQL';

SELECT LOCATE('world', 'hello world');
SELECT LOCATE('hello', 'hello world');
SELECT LOCATE('moon', 'hello world');
SELECT LOCATE('', 'hello world');
SELECT LOCATE('hello world', 'hello world');
SELECT LOCATE('world', 'hello world', 8);
SELECT LOCATE('world', 'hello world', 5);
SELECT LOCATE('world', 'hello world', 10);
SELECT LOCATE('world', '');
SELECT LOCATE(NULL, 'hello world');
SELECT LOCATE('world', NULL);
SELECT LOCATE(NULL, NULL);

set enable_optimizer=1;
set dialect_type='MYSQL';
DROP TABLE IF EXISTS table_locate_1;
CREATE TABLE table_locate_1(val1 UInt64, val2 String,val3 String) ENGINE=CnchMergeTree() ORDER BY val1;
INSERT INTO table_locate_1 VALUES(0, 'bar', 'foobarbar');
INSERT INTO table_locate_1 VALUES(1, 'ar', 'bar');
INSERT INTO table_locate_1 VALUES(2, 'foobarbar', 'x');
SELECT val1, LOCATE(val2, val3), LOCATE(val2, val3, val1) FROM table_locate_1 ORDER BY val1 ASC;
DROP TABLE table_locate_1;
