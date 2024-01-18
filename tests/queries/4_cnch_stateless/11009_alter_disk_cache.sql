USE test;
DROP TABLE IF EXISTS at_dc;
CREATE TABLE at_dc(a UInt32, p UInt32) ENGINE = CnchMergeTree ORDER BY a PARTITION BY p SETTINGS parts_preload_level = 1;
INSERT INTO at_dc VALUES (1, 1), (2, 1), (3, 1);
INSERT INTO at_dc VALUES (4, 2), (5, 2), (6, 2);

ALTER DISK CACHE PRELOAD TABLE test.at_dc PARTITION 1 SYNC SETTINGS parts_preload_level = 3;
SELECT a FROM at_dc WHERE p = 1 ORDER BY a SETTINGS disk_cache_mode = 'FORCE_DISK_CACHE';

ALTER DISK CACHE PRELOAD TABLE test.at_dc SYNC SETTINGS parts_preload_level = 3;
SELECT a FROM at_dc ORDER BY a SETTINGS disk_cache_mode = 'FORCE_DISK_CACHE';

ALTER DISK CACHE DROP TABLE test.at_dc PARTITION 1 SYNC;
SELECT a FROM at_dc WHERE p = 1 ORDER BY a SETTINGS disk_cache_mode = 'FORCE_DISK_CACHE'; -- { serverError 5046 } --

DROP TABLE at_dc;

DROP TABLE IF EXISTS test_bucket_preload;
CREATE TABLE test_bucket_preload(a UInt32, p UInt32, c UInt32) ENGINE = CnchMergeTree ORDER BY a PARTITION BY p CLUSTER BY c INTO 3 BUCKETS SETTINGS parts_preload_level = 1;
INSERT INTO test_bucket_preload SELECT number, 1, number % 7 FROM numbers(10);
SELECT '---bucket---';

ALTER DISK CACHE PRELOAD TABLE test.test_bucket_preload SYNC SETTINGS parts_preload_level = 3;
SELECT a FROM test_bucket_preload ORDER BY a SETTINGS disk_cache_mode = 'FORCE_DISK_CACHE';

ALTER DISK CACHE DROP TABLE test.test_bucket_preload SYNC SETTINGS drop_vw_disk_cache = 1;
SELECT a FROM test_bucket_preload WHERE p = 1 ORDER BY a SETTINGS disk_cache_mode = 'FORCE_DISK_CACHE'; -- { serverError 5046 } --

DROP TABLE test_bucket_preload
