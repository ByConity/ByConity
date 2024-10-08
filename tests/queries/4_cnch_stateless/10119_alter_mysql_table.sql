SET show_table_uuid_in_table_create_query_if_not_nil = 0;

DROP TABLE IF EXISTS mysql_test;
CREATE TABLE mysql_test(x Int32) Engine = MySQL('127.0.0.1:3306', 'test_db', 'test_tb', 'user', 'password');
ALTER TABLE mysql_test Engine = MySQL('127.0.0.2:3306', 'test_db', 'test_tb', 'user', 'password');
ALTER TABLE mysql_test Engine = MySQL('127.0.0.2:3306', 'test_db', 'test_tb');  -- { serverError 42 }
ALTER TABLE mysql_test Engine = CnchMergeTree; -- { serverError 36 }

SHOW CREATE TABLE mysql_test;
DROP TABLE mysql_test;

DROP TABLE IF EXISTS cnch_table;
CREATE TABLE cnch_table(p_date Date) Engine = CnchMergeTree ORDER BY tuple();
ALTER TABLE cnch_table Engine = MySQL('127.0.0.1:3306', 'test_db', 'test_tb', 'user', 'password'); -- { serverError 48 }

DROP TABLE cnch_table;

