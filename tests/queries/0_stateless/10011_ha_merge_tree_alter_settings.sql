DROP TABLE IF EXISTS ha_table_for_alter1;
DROP TABLE IF EXISTS ha_table_for_alter2;

SET replication_alter_partitions_sync = 2;

CREATE TABLE ha_table_for_alter1 (
  id UInt64,
  Data String
) ENGINE = HaMergeTree('/clickhouse/tables/test_10011_{database}/ha_table_for_alter', '1') ORDER BY id;

CREATE TABLE ha_table_for_alter2 (
  id UInt64,
  Data String
) ENGINE = HaMergeTree('/clickhouse/tables/test_10011_{database}/ha_table_for_alter', '2') ORDER BY id;

SHOW CREATE TABLE ha_table_for_alter1;

ALTER TABLE ha_table_for_alter1 MODIFY SETTING index_granularity = 4096; -- { serverError 472 }

SHOW CREATE TABLE ha_table_for_alter1;

INSERT INTO ha_table_for_alter2 VALUES (1, '1'), (2, '2');

SYSTEM SYNC REPLICA ha_table_for_alter1;

ALTER TABLE ha_table_for_alter1 MODIFY SETTING use_minimalistic_part_header_in_zookeeper = 1;

INSERT INTO ha_table_for_alter1 VALUES (3, '3'), (4, '4');

SYSTEM SYNC REPLICA ha_table_for_alter2;

SELECT COUNT() FROM ha_table_for_alter1;
SELECT COUNT() FROM ha_table_for_alter2;

DETACH TABLE ha_table_for_alter2;
ATTACH TABLE ha_table_for_alter2;

DETACH TABLE ha_table_for_alter1;
ATTACH TABLE ha_table_for_alter1;

SELECT COUNT() FROM ha_table_for_alter1;
SELECT COUNT() FROM ha_table_for_alter2;

ALTER TABLE ha_table_for_alter2 MODIFY SETTING  parts_to_throw_insert = 1, parts_to_delay_insert = 1;
INSERT INTO ha_table_for_alter2 VALUES (3, '1'), (4, '2'); -- { serverError 252 }

INSERT INTO ha_table_for_alter1 VALUES (5, '5'), (6, '6');

SYSTEM SYNC REPLICA ha_table_for_alter2;

SELECT COUNT() FROM ha_table_for_alter1;
SELECT COUNT() FROM ha_table_for_alter2;

DETACH TABLE ha_table_for_alter2;
ATTACH TABLE ha_table_for_alter2;

DETACH TABLE ha_table_for_alter1;
ATTACH TABLE ha_table_for_alter1;

SHOW CREATE TABLE ha_table_for_alter1;
SHOW CREATE TABLE ha_table_for_alter2;

ALTER TABLE ha_table_for_alter1 ADD COLUMN Data2 UInt64, MODIFY SETTING check_delay_period=5, check_delay_period=10, check_delay_period=15;

SHOW CREATE TABLE ha_table_for_alter1;
SHOW CREATE TABLE ha_table_for_alter2;

DROP TABLE IF EXISTS ha_table_for_alter2;
DROP TABLE IF EXISTS ha_table_for_alter1;

DROP TABLE IF EXISTS ha_table_for_reset_setting1;
DROP TABLE IF EXISTS ha_table_for_reset_setting2;

SET replication_alter_partitions_sync = 2;

CREATE TABLE ha_table_for_reset_setting1 (
 id UInt64,
 Data String
) ENGINE = HaMergeTree('/clickhouse/tables/test_10011_{database}/ha_table_for_reset_setting', '1') ORDER BY id;

CREATE TABLE ha_table_for_reset_setting2 (
 id UInt64,
 Data String
) ENGINE = HaMergeTree('/clickhouse/tables/test_10011_{database}/ha_table_for_reset_setting', '2') ORDER BY id;

SHOW CREATE TABLE ha_table_for_reset_setting1;
SHOW CREATE TABLE ha_table_for_reset_setting2;

ALTER TABLE ha_table_for_reset_setting1 MODIFY SETTING index_granularity = 4096; -- { serverError 472 }

SHOW CREATE TABLE ha_table_for_reset_setting1;

ALTER TABLE ha_table_for_reset_setting1 MODIFY SETTING merge_with_ttl_timeout = 100;
ALTER TABLE ha_table_for_reset_setting2 MODIFY SETTING merge_with_ttl_timeout = 200;

SHOW CREATE TABLE ha_table_for_reset_setting1;
SHOW CREATE TABLE ha_table_for_reset_setting2;

DETACH TABLE ha_table_for_reset_setting2;
ATTACH TABLE ha_table_for_reset_setting2;

DETACH TABLE ha_table_for_reset_setting1;
ATTACH TABLE ha_table_for_reset_setting1;

SHOW CREATE TABLE ha_table_for_reset_setting1;
SHOW CREATE TABLE ha_table_for_reset_setting2;

-- ignore undefined setting
ALTER TABLE ha_table_for_reset_setting1 RESET SETTING check_delay_period, unknown_setting;
ALTER TABLE ha_table_for_reset_setting1 RESET SETTING merge_with_ttl_timeout;
ALTER TABLE ha_table_for_reset_setting2 RESET SETTING merge_with_ttl_timeout;

SHOW CREATE TABLE ha_table_for_reset_setting1;
SHOW CREATE TABLE ha_table_for_reset_setting2;

DROP TABLE IF EXISTS ha_table_for_reset_setting2;
DROP TABLE IF EXISTS ha_table_for_reset_setting1;
