SELECT '-- construct test db1 and tables --';
CREATE DATABASE IF NOT EXISTS test_cnch_ddl_check_db1;
CREATE TABLE IF NOT EXISTS test_cnch_ddl_check_db1.tb1(d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;

SELECT '-- test modify String to Array(String) --';
alter table test_cnch_ddl_check_db1.tb1 modify column a Array(String); --{serverError 53} CAST AS Array can only be performed between same-dimensional Array or String types

SELECT '-- clean --';
DROP DATABASE test_cnch_ddl_check_db1;
