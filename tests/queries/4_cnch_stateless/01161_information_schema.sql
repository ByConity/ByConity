show tables from information_schema;
SHOW TABLES FROM INFORMATION_SCHEMA;
CREATE DATABASE IF NOT EXISTS 01161_information_schema;
use 01161_information_schema;
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS t;
--DROP VIEW IF EXISTS mv;
DROP TABLE IF EXISTS tmp;
DROP TABLE IF EXISTS kcu;
DROP TABLE IF EXISTS kcu2;
DROP TABLE IF EXISTS partitioned;

CREATE TABLE t (n UInt64, f Float32, s String, fs FixedString(42), d Decimal(9, 6)) ENGINE = CnchMergeTree() ORDER BY tuple();
CREATE VIEW v (n Nullable(Int32), f Float64) AS SELECT n, f FROM t;
--CREATE MATERIALIZED VIEW mv ENGINE=Null AS SELECT * FROM system.one;
CREATE TEMPORARY TABLE tmp (d Date, dt DateTime, dtms DateTime64(3));
CREATE TABLE kcu (i UInt32, s String) ENGINE = CnchMergeTree() ORDER BY i;
CREATE TABLE kcu2 (i UInt32, d Date, u UUID) ENGINE = CnchMergeTree() ORDER BY (u, d);
CREATE TABLE partitioned (i UInt32, s String) ENGINE = CnchMergeTree() PARTITION BY i % 3 ORDER BY tuple();
SET enable_optimizer=0;

-- FIXME #28687
select * from information_schema.schemata where schema_name ilike 'information_schema';
-- SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA=currentDatabase(1) OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%';
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (table_schema = currentDatabase(1) OR table_schema = '') AND table_name NOT LIKE '%inner%';
SELECT * FROM information_schema.views WHERE table_schema = currentDatabase(1);
-- SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (TABLE_SCHEMA=currentDatabase(1) OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%'
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (table_schema = currentDatabase(1) OR table_schema = '') AND table_name NOT LIKE '%inner%';
SET enable_optimizer=1;
-- FIXME #28687
select * from information_schema.schemata where schema_name ilike 'information_schema';
-- SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA=currentDatabase(1) OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%';
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (table_schema = currentDatabase(1) OR table_schema = '') AND table_name NOT LIKE '%inner%';
SELECT * FROM information_schema.views WHERE table_schema = currentDatabase(1);
-- SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (TABLE_SCHEMA=currentDatabase(1) OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%'
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (table_schema = currentDatabase(1) OR table_schema = '') AND table_name NOT LIKE '%inner%';
SET enable_optimizer=0;
-- mixed upper/lowercase schema and table name:
SELECT count() FROM information_schema.TABLES WHERE table_schema = currentDatabase(1) AND table_name = 't';
SELECT count() FROM INFORMATION_SCHEMA.tables WHERE table_schema = currentDatabase(1) AND table_name = 't';

SELECT * FROM information_schema.key_column_usage WHERE table_schema = currentDatabase(1) AND table_name = 'kcu';
SELECT * FROM information_schema.key_column_usage WHERE table_schema = currentDatabase(1) AND table_name = 'kcu2';

SELECT '-- information_schema.referential_constraints';
SELECT * FROM information_schema.referential_constraints;

SELECT '-- information_schema.statistics';
SELECT * FROM information_schema.statistics WHERE table_schema = currentDatabase() order by table_name;

SELECT '-- information_schema.events';
SELECT * FROM information_schema.events;

SELECT '-- information_schema.routines';
SELECT * FROM information_schema.routines;

SELECT '-- information_schema.triggers';
SELECT * FROM information_schema.triggers;

SELECT '-- information_schema.partitions';
SELECT * FROM information_schema.partitions WHERE table_schema = currentDatabase(1) > 0;
INSERT INTO partitioned VALUES
    (0, 'zero'), (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five'), (1000, 'one thousand');

SELECT '-- information_schema.partitions (After INSERT)';
SELECT table_name, partition_name, partition_expression, table_rows FROM information_schema.partitions WHERE table_schema = currentDatabase(1) ORDER BY table_name, partition_name SETTINGS enable_multiple_tables_for_cnch_parts=1;
SELECT table_name, partition_name, partition_expression, table_rows FROM information_schema.partitions WHERE table_schema = currentDatabase(1) ORDER BY table_name, partition_name SETTINGS dialect_type='ANSI',enable_multiple_tables_for_cnch_parts=1;
SELECT table_name, partition_name, partition_expression, table_rows FROM information_schema.partitions WHERE table_schema = currentDatabase(1) ORDER BY table_name, partition_name SETTINGS dialect_type='MYSQL',enable_multiple_tables_for_cnch_parts=1;

SELECT '-- information_schema.engines';
SELECT * FROM information_schema.engines ORDER BY engine;

SELECT '-- information_schema.profiling';
SELECT * FROM information_schema.profiling;

SELECT '-- information_schema.files';
SELECT * FROM information_schema.files;

--drop view mv;
drop view v;
drop table t;
drop table kcu;
drop table kcu2;
drop table partitioned;
drop database 01161_information_schema;
