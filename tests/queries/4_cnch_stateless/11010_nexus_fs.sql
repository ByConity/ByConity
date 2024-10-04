USE test;
set bsp_max_retry_num=0; -- disable bsp retry
set enable_nexus_fs=1;

DROP TABLE IF EXISTS 11010_nexus_fs;

SELECT '---directly read---';
CREATE TABLE 11010_nexus_fs (d Decimal(4, 3)) ENGINE = CnchMergeTree ORDER BY d SETTINGS index_granularity = 1, enable_local_disk_cache = 0, enable_nexus_fs = 1;
INSERT INTO 11010_nexus_fs SELECT toDecimal64(number, 3) FROM numbers(10000);

SELECT d FROM 11010_nexus_fs WHERE toFloat64(d) = 7777.0;
SELECT d FROM 11010_nexus_fs WHERE toFloat64(d) = 8888.0;

DROP TABLE 11010_nexus_fs;

SELECT '---with preload---';
CREATE TABLE 11010_nexus_fs (d Decimal(4, 3)) ENGINE = CnchMergeTree ORDER BY d SETTINGS index_granularity = 1, enable_local_disk_cache = 0, enable_nexus_fs = 1;
INSERT INTO 11010_nexus_fs SELECT toDecimal64(number, 3) FROM numbers(10000);

ALTER DISK CACHE PRELOAD TABLE test.11010_nexus_fs SYNC SETTINGS parts_preload_level = 3;

SELECT d FROM 11010_nexus_fs WHERE toFloat64(d) = 7777.0;
SELECT d FROM 11010_nexus_fs WHERE toFloat64(d) = 8888.0;

DROP TABLE 11010_nexus_fs;
