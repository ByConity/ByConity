SELECT '-- construct test db1 and tables --';
CREATE DATABASE IF NOT EXISTS test_cnch_50002_db1;
DROP TABLE IF EXISTS test_cnch_50002_db1.tb1;
CREATE TABLE IF NOT EXISTS test_cnch_50002_db1.tb1 (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;

SELECT '-- construct test db2 and tables --';
CREATE DATABASE IF NOT EXISTS test_cnch_50002_db2;
DROP TABLE IF EXISTS test_cnch_50002_db2.tb1;
CREATE TABLE IF NOT EXISTS test_cnch_50002_db2.tb1(d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;
DROP TABLE IF EXISTS test_cnch_50002_db2.tb2;
CREATE TABLE IF NOT EXISTS test_cnch_50002_db2.tb2(d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;

SELECT '-- construct test db3 and tables --';
CREATE DATABASE IF NOT EXISTS test_cnch_50002_db3;
DROP TABLE IF EXISTS test_cnch_50002_db3.tb1;
CREATE TABLE IF NOT EXISTS test_cnch_50002_db3.tb1(d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;
DROP TABLE IF EXISTS test_cnch_50002_db3.tb2;
CREATE TABLE IF NOT EXISTS test_cnch_50002_db3.tb2(d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;
DROP TABLE IF EXISTS test_cnch_50002_db3.tb3;
CREATE TABLE IF NOT EXISTS test_cnch_50002_db3.tb3(d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;

SELECT '-- test count --';
SELECT COUNT(*) FROM system.cnch_tables where database like '%test_cnch_50002_db%';
SELECT '-- test single table query --';
SELECT database, name FROM system.cnch_tables where database='test_cnch_50002_db2' AND name='tb1';
SELECT '-- test multiple tables or query --';
SELECT database, name FROM system.cnch_tables where (database='test_cnch_50002_db1' AND name='tb1') OR (database='test_cnch_50002_db3' AND name='tb2') ORDER BY database, name;
SELECT '-- test db and db_table mixed query --';
SELECT database, name FROM system.cnch_tables where (database like '%test_cnch_50002_db%') OR (database='test_cnch_50002_db3' AND name='tb3') ORDER BY database, name;

SELECT '-- clean --';
DROP TABLE test_cnch_50002_db1.tb1;
DROP TABLE test_cnch_50002_db2.tb1;
DROP TABLE test_cnch_50002_db2.tb2;
DROP TABLE test_cnch_50002_db3.tb1;
DROP TABLE test_cnch_50002_db3.tb2;
DROP TABLE test_cnch_50002_db3.tb3;

DROP DATABASE test_cnch_50002_db1;
DROP DATABASE test_cnch_50002_db2;
DROP DATABASE test_cnch_50002_db3;
