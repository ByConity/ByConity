SET ha_alter_data_sync = 1;

DROP TABLE IF EXISTS test.ha_lost1;
DROP TABLE IF EXISTS test.ha_lost2;

CREATE TABLE test.ha_lost1 (d Date, k UInt64, i32 Int32) ENGINE = HaMergeTree('/clickhouse/tables/test/ha_lost', 'r1') PARTITION BY d ORDER BY k SETTINGS ha_mark_lost_replica_timeout = 1, ha_update_replica_stats_min_period = 1, ha_update_replica_stats_period = 1, ha_queue_update_sleep_ms = 1000;
CREATE TABLE test.ha_lost2 (d Date, k UInt64, i32 Int32) ENGINE = HaMergeTree('/clickhouse/tables/test/ha_lost', 'r2') PARTITION BY d ORDER BY k SETTINGS zk_local_diff_threshold = 4, ha_queue_update_sleep_ms = 1000;


INSERT INTO test.ha_lost1 VALUES (0, 0, 0);

SYSTEM MARK LOST test.ha_lost2;
DETACH TABLE test.ha_lost2;
SELECT sleep(1.2);

ALTER TABLE test.ha_lost1 ADD COLUMN s String, MODIFY COLUMN i32 String;
INSERT INTO test.ha_lost1 VALUES (0, 3, '1', 'aaa');

ATTACH TABLE test.ha_lost2;

SHOW CREATE test.ha_lost1;
SHOW CREATE test.ha_lost2;
DESC test.ha_lost1;
DESC test.ha_lost2;

SYSTEM SYNC REPLICA test.ha_lost2;

SELECT * FROM test.ha_lost1 ORDER BY k;
SELECT * FROM test.ha_lost2 ORDER BY k;
