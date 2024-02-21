DROP DATABASE IF EXISTS test_show_tables;

CREATE DATABASE test_show_tables;

DROP VIEW IF EXISTS test_show_tables.V;
DROP TABLE IF EXISTS test_show_tables.A;
DROP TABLE IF EXISTS test_show_tables.B;

CREATE TABLE test_show_tables.A (A UInt8) ENGINE=CnchMergeTree() ORDER BY A;
CREATE TABLE test_show_tables.B (A UInt8) ENGINE=CnchMergeTree() ORDER BY A;
CREATE VIEW test_show_tables.V (V UInt8) AS SELECT A.A AS V FROM test_show_tables.A;

SHOW TABLES from test_show_tables;
SHOW FULL TABLES from test_show_tables;

DROP VIEW test_show_tables.V;
DROP TABLE test_show_tables.A;
DROP TABLE test_show_tables.B;

SHOW FULL TABLES from system LIKE 'numbers';
-- To be added after information schema
-- SHOW FULL TABLES from information_schema LIKE 'numbers';
-- SHOW FULL TABLES from INFORMATION_SCHEMA LIKE 'numbers';
