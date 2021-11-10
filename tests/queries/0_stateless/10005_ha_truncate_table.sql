DROP TABLE IF EXISTS ha_truncate1;
DROP TABLE IF EXISTS ha_truncate2;

CREATE TABLE ha_truncate1 (d Date, k UInt64, i32 Int32) ENGINE=HaMergeTree('/clickhouse/tables/test_10005/truncate', 'r1', d, k, 8192);
CREATE TABLE ha_truncate2 (d Date, k UInt64, i32 Int32) ENGINE=HaMergeTree('/clickhouse/tables/test_10005/truncate', 'r2', d, k, 8192);

SELECT '======Before Truncate======';
INSERT INTO ha_truncate1 VALUES ('2015-01-01', 10, 42);

SYSTEM SYNC REPLICA ha_truncate2;

SELECT * FROM ha_truncate1 ORDER BY k;
SELECT * FROM ha_truncate2 ORDER BY k;

SELECT '======After Truncate And Empty======';
TRUNCATE TABLE ha_truncate1;

SELECT * FROM ha_truncate1 ORDER BY k;
SELECT * FROM ha_truncate2 ORDER BY k;

SELECT '======After Truncate And Insert Data======';
INSERT INTO ha_truncate1 VALUES ('2015-01-01', 10, 42);

SYSTEM SYNC REPLICA ha_truncate2;

SELECT * FROM ha_truncate1 ORDER BY k;
SELECT * FROM ha_truncate2 ORDER BY k;

DROP TABLE IF EXISTS ha_truncate1;
DROP TABLE IF EXISTS ha_truncate2;
