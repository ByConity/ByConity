USE test;
set bsp_max_retry_num=0; -- disable bsp retry
DROP TABLE IF EXISTS at_dc;
CREATE TABLE at_dc(a UInt32, p UInt32) ENGINE = CnchMergeTree ORDER BY a PARTITION BY p SETTINGS enable_nexus_fs = 0;
INSERT INTO at_dc VALUES (1, 1), (2, 1), (3, 1);
INSERT INTO at_dc VALUES (4, 2), (5, 2), (6, 2);

ALTER TABLE at_dc MODIFY SETTING parts_preload_level = 1;
ALTER DISK CACHE PRELOAD TABLE test.at_dc ASYNC SETTINGS parts_preload_level = 3, remote_fs_read_failed_injection = -1; -- preload will be failed since remote_fs_read_failed_injection = -1
select sleepEachRow(3) from system.numbers limit 2 format Null;
SELECT a FROM at_dc WHERE p = 1 ORDER BY a SETTINGS disk_cache_mode = 'FORCE_DISK_CACHE'; -- { serverError 5046 } --

ALTER DISK CACHE PRELOAD TABLE test.at_dc PARTITION 1 SETTINGS parts_preload_level = 3;
select sleepEachRow(3) from system.numbers limit 2 format Null;
SELECT a FROM at_dc WHERE p = 1 ORDER BY a SETTINGS disk_cache_mode = 'FORCE_DISK_CACHE';

ALTER DISK CACHE PRELOAD TABLE test.at_dc SYNC SETTINGS parts_preload_level = 3;
SELECT a FROM at_dc ORDER BY a SETTINGS disk_cache_mode = 'FORCE_DISK_CACHE';

ALTER DISK CACHE DROP TABLE test.at_dc PARTITION 1 SYNC;
SELECT a FROM at_dc WHERE p = 1 ORDER BY a SETTINGS disk_cache_mode = 'FORCE_DISK_CACHE'; -- { serverError 5046 } --

DROP TABLE at_dc;

DROP TABLE IF EXISTS test_bucket_preload;
CREATE TABLE test_bucket_preload(a UInt32, p UInt32, c UInt32) ENGINE = CnchMergeTree ORDER BY a PARTITION BY p CLUSTER BY c INTO 3 BUCKETS SETTINGS parts_preload_level = 1, enable_nexus_fs = 0;
INSERT INTO test_bucket_preload SELECT number, 1, number % 7 FROM numbers(10);
SELECT '---bucket---';

ALTER DISK CACHE PRELOAD TABLE test.test_bucket_preload SYNC SETTINGS parts_preload_level = 3;
SELECT a FROM test_bucket_preload ORDER BY a SETTINGS disk_cache_mode = 'FORCE_DISK_CACHE';

ALTER DISK CACHE DROP TABLE test.test_bucket_preload SYNC SETTINGS drop_vw_disk_cache = 1;
SELECT a FROM test_bucket_preload WHERE p = 1 ORDER BY a SETTINGS disk_cache_mode = 'FORCE_DISK_CACHE'; -- { serverError 5046 } --

DROP TABLE test_bucket_preload;


DROP TABLE IF EXISTS 11009_alter_disk_cache;

SELECT '---all segments stores in single compressed block---';
CREATE TABLE 11009_alter_disk_cache (d Decimal(4, 3)) ENGINE = CnchMergeTree ORDER BY d SETTINGS index_granularity = 1, parts_preload_level = 1, enable_nexus_fs = 0;
INSERT INTO 11009_alter_disk_cache SELECT toDecimal64(number, 3) FROM numbers(10000);
ALTER DISK CACHE PRELOAD TABLE test.11009_alter_disk_cache SYNC SETTINGS parts_preload_level = 3;
SELECT d FROM 11009_alter_disk_cache WHERE toFloat64(d) = 7777.0 settings disk_cache_mode = 'FORCE_DISK_CACHE';
SELECT d FROM 11009_alter_disk_cache WHERE toFloat64(d) = 8888.0 settings disk_cache_mode = 'FORCE_DISK_CACHE';

DROP TABLE 11009_alter_disk_cache;

