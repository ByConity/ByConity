
DROP TABLE IF EXISTS test.ha_con_alter1;
DROP TABLE IF EXISTS test.ha_con_alter2;

CREATE TABLE test.ha_con_alter1 (k Int64, v String, d Date) ENGINE = HaMergeTree('/clickhouse/tables/test/ha_con_alter', 'r1') ORDER BY k PARTITION BY d SETTINGS ha_update_replica_stats_min_period = 1, ha_update_replica_stats_period = 1, ha_queue_update_sleep_ms = 1000;
CREATE TABLE test.ha_con_alter2 (k Int64, v String, d Date) ENGINE = HaMergeTree('/clickhouse/tables/test/ha_con_alter', 'r2') ORDER BY k PARTITION BY d SETTINGS ha_update_replica_stats_min_period = 1, ha_update_replica_stats_period = 1, ha_queue_update_sleep_ms = 1000;

INSERT INTO test.ha_con_alter1 VALUES (0, '1', 0);
INSERT INTO test.ha_con_alter1 VALUES (0, '1', 1);
INSERT INTO test.ha_con_alter1 VALUES (0, '1', 2);
INSERT INTO test.ha_con_alter1 VALUES (0, '1', 3);
INSERT INTO test.ha_con_alter1 VALUES (0, '1', 4);
INSERT INTO test.ha_con_alter1 VALUES (0, '1', 5);
INSERT INTO test.ha_con_alter1 VALUES (0, '1', 6);
INSERT INTO test.ha_con_alter1 VALUES (0, '1', 7);
INSERT INTO test.ha_con_alter1 VALUES (0, '1', 8);
INSERT INTO test.ha_con_alter1 VALUES (0, '1', 9);

ALTER TABLE test.ha_con_alter1 MODIFY column v Int64 SETTINGS ha_alter_data_sync = 2;
ALTER TABLE test.ha_con_alter2 MODIFY column v Int64 SETTINGS ha_alter_data_sync = 2;

SHOW CREATE test.ha_con_alter1;
SHOW CREATE test.ha_con_alter2;

ALTER TABLE test.ha_con_alter1 MODIFY column v String;
ALTER TABLE test.ha_con_alter2 MODIFY column v UInt64; -- { serverError 517 }

SYSTEM SYNC MUTATION test.ha_con_alter1;
SYSTEM SYNC MUTATION test.ha_con_alter2;

ALTER TABLE test.ha_con_alter1 MODIFY column v String;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v00 Int;

SHOW CREATE test.ha_con_alter1;
SHOW CREATE test.ha_con_alter2;

ALTER TABLE test.ha_con_alter1 ADD COLUMN v01 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v02 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v03 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v04 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v05 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v06 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v07 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v08 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v09 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v10 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v11 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v12 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v13 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v14 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v15 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v16 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v17 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v18 Int;
ALTER TABLE test.ha_con_alter1 ADD COLUMN v19 Int;

SHOW CREATE test.ha_con_alter1;
SYSTEM SYNC MUTATION test.ha_con_alter2;
SHOW CREATE test.ha_con_alter2;

SELECT * FROM test.ha_con_alter1 ORDER BY d;

DROP TABLE test.ha_con_alter1;
DROP TABLE test.ha_con_alter2;
