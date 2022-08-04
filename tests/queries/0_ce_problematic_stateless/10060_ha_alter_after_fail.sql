SET ha_alter_data_sync = 1;

DROP TABLE IF EXISTS test.ha_fail1;
DROP TABLE IF EXISTS test.ha_fail2;

CREATE TABLE test.ha_fail1 (d Date, k UInt64, i32 Int32) ENGINE = HaMergeTree('/clickhouse/tables/test/ha_fail', 'r1') PARTITION BY d ORDER BY k SETTINGS ha_queue_update_sleep_ms = 1000;
CREATE TABLE test.ha_fail2 (d Date, k UInt64, i32 Int32) ENGINE = HaMergeTree('/clickhouse/tables/test/ha_fail', 'r2') PARTITION BY d ORDER BY k SETTINGS ha_queue_update_sleep_ms = 1000;

INSERT INTO test.ha_fail1 VALUES (0, 0, 0);

DETACH TABLE test.ha_fail2;

ALTER TABLE test.ha_fail1 ADD COLUMN s String, MODIFY COLUMN i32 String;
INSERT INTO test.ha_fail1 VALUES (0, 3, '1', 'aaa');

ATTACH TABLE test.ha_fail2;

SYSTEM SYNC REPLICA test.ha_fail2;
SYSTEM SYNC MUTATION test.ha_fail2;

SHOW CREATE test.ha_fail1;
SHOW CREATE test.ha_fail2;
DESC test.ha_fail1;
DESC test.ha_fail2;

SELECT * FROM test.ha_fail1 ORDER BY k;
SELECT * FROM test.ha_fail2 ORDER BY k;

DROP TABLE test.ha_fail1;
DROP TABLE test.ha_fail2;
