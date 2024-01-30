show tables from information_schema;
SHOW TABLES FROM INFORMATION_SCHEMA;
DROP TABLE IF EXISTS t;
DROP VIEW IF EXISTS v;
--DROP VIEW IF EXISTS mv;
DROP TABLE IF EXISTS tmp;
DROP TABLE IF EXISTS kcu;
DROP TABLE IF EXISTS kcu2;

CREATE TABLE t (n UInt64, f Float32, s String, fs FixedString(42), d Decimal(9, 6)) ENGINE = CnchMergeTree() ORDER BY tuple();
CREATE VIEW v (n Nullable(Int32), f Float64) AS SELECT n, f FROM t;
--CREATE MATERIALIZED VIEW mv ENGINE=Null AS SELECT * FROM system.one;
CREATE TEMPORARY TABLE tmp (d Date, dt DateTime, dtms DateTime64(3));
CREATE TABLE kcu (i UInt32, s String) ENGINE = CnchMergeTree() ORDER BY i;
CREATE TABLE kcu2 (i UInt32, d Date, u UUID) ENGINE = CnchMergeTree() ORDER BY (u, d);
SET enable_optimizer=0;

-- FIXME #28687
select * from information_schema.schemata where schema_name ilike 'information_schema';
-- SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA=currentDatabase() OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%';
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (table_schema = currentDatabase() OR table_schema = '') AND table_name NOT LIKE '%inner%';
SELECT * FROM information_schema.views WHERE table_schema = currentDatabase();
-- SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (TABLE_SCHEMA=currentDatabase() OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%'
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (table_schema = currentDatabase() OR table_schema = '') AND table_name NOT LIKE '%inner%';
SET enable_optimizer=1;
-- FIXME #28687
select * from information_schema.schemata where schema_name ilike 'information_schema';
-- SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA=currentDatabase() OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%';
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (table_schema = currentDatabase() OR table_schema = '') AND table_name NOT LIKE '%inner%';
SELECT * FROM information_schema.views WHERE table_schema = currentDatabase();
-- SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (TABLE_SCHEMA=currentDatabase() OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%'
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (table_schema = currentDatabase() OR table_schema = '') AND table_name NOT LIKE '%inner%';
SET enable_optimizer=0;
-- mixed upper/lowercase schema and table name:
SELECT count() FROM information_schema.TABLES WHERE table_schema = currentDatabase() AND table_name = 't';
SELECT count() FROM INFORMATION_SCHEMA.tables WHERE table_schema = currentDatabase() AND table_name = 't';
SELECT count() FROM INFORMATION_schema.tables WHERE table_schema = currentDatabase() AND table_name = 't'; -- { serverError 81 }
SELECT count() FROM information_schema.taBLES WHERE table_schema = currentDatabase() AND table_name = 't'; -- { serverError 60 }

SELECT * FROM information_schema.key_column_usage WHERE table_schema = currentDatabase() AND table_name = 'kcu';
SELECT * FROM information_schema.key_column_usage WHERE table_schema = currentDatabase() AND table_name = 'kcu2';

SELECT * FROM information_schema.referential_constraints;

--drop view mv;
drop view v;
drop table t;
drop table kcu;
drop table kcu2;
