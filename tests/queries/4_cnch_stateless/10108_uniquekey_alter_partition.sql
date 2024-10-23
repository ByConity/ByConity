DROP TABLE IF EXISTS u10108_pl;

CREATE TABLE u10108_pl (d Date, k1 Int64, k2 Int64, v1 Int64) ENGINE = CnchMergeTree() PARTITION BY d ORDER BY k1 UNIQUE KEY (k1, k2);

INSERT INTO u10108_pl SELECT '2021-01-01', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-01', 2, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-02', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-02', 2, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-03', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-03', 2, number, number FROM system.numbers LIMIT 100;

SELECT 'test drop partition: ';
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;

-- drop partition
SELECT 'drop partition 2021-01-01';
ALTER TABLE u10108_pl DROP PARTITION '2021-01-01';
INSERT INTO u10108_pl SELECT '2021-01-01', 3, number, number FROM system.numbers LIMIT 100;
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;

-- drop partition where
SELECT 'drop partition where d=\'2021-01-02\'';
ALTER TABLE u10108_pl DROP PARTITION WHERE d = '2021-01-02';
INSERT INTO u10108_pl SELECT '2021-01-02', 3, number, number FROM system.numbers LIMIT 100;
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;

DROP TABLE IF EXISTS u10108_pl;
DROP TABLE IF EXISTS u10108_pl_helper;

SELECT '';
CREATE TABLE u10108_pl (d Date, k1 Int64, k2 Int64, v1 Int64) ENGINE = CnchMergeTree() PARTITION BY d ORDER BY k1 UNIQUE KEY (k1, k2);
CREATE TABLE u10108_pl_helper (d Date, k1 Int64, k2 Int64, v1 Int64) ENGINE = CnchMergeTree() PARTITION BY d ORDER BY k1 UNIQUE KEY (k1, k2);

INSERT INTO u10108_pl SELECT '2023-01-01', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2023-01-01', 1, number, number FROM system.numbers LIMIT 50;
INSERT INTO u10108_pl_helper SELECT '2023-01-01', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl_helper SELECT '2023-01-01', 1, number, number FROM system.numbers LIMIT 50;

SELECT 'test detach/attach partition: ';
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;
SELECT 'detach partition 2023-01-01';
ALTER TABLE u10108_pl DETACH PARTITION '2023-01-01';
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;
SELECT 'attach partition 2023-01-01';
ALTER TABLE u10108_pl ATTACH PARTITION '2023-01-01';
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;

SELECT 'u10108_pl_helper table detach partition 2023-01-01';
ALTER TABLE u10108_pl_helper DETACH PARTITION '2023-01-01';
SELECT d, k1, count(1), sum(v1) FROM u10108_pl_helper GROUP BY d, k1 ORDER BY d, k1;
SELECT 'attach detached partition 2023-01-01 from u10108_pl_helper';
ALTER TABLE u10108_pl ATTACH DETACHED PARTITION '2023-01-01' FROM u10108_pl_helper;
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;

SET enable_unique_table_attach_without_dedup = 1;
SELECT 'attach detached partition 2023-01-01 from u10108_pl_helper without dedup, it will has duplicated data';
INSERT INTO u10108_pl_helper SELECT '2023-01-01', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl_helper SELECT '2023-01-01', 1, number, number FROM system.numbers LIMIT 50;
ALTER TABLE u10108_pl_helper DETACH PARTITION '2023-01-01';
ALTER TABLE u10108_pl ATTACH DETACHED PARTITION '2023-01-01' FROM u10108_pl_helper;
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;

SELECT 'using repair command to dedup data';
SYSTEM DEDUP u10108_pl PARTITION '2023-01-01' FOR REPAIR;
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;

SELECT 'u10108_pl_helper table detach partition without bitmap';
SET enable_unique_table_detach_ignore_delete_bitmap = 1;
INSERT INTO u10108_pl_helper SELECT '2023-01-01', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl_helper SELECT '2023-01-01', 1, number, number FROM system.numbers LIMIT 50;
ALTER TABLE u10108_pl_helper DETACH PARTITION '2023-01-01';
SELECT 'attach detached partition 2023-01-01 from u10108_pl_helper without dedup, it will not has duplicated data because part to be attached does not have bitmap';
ALTER TABLE u10108_pl ATTACH DETACHED PARTITION '2023-01-01' FROM u10108_pl_helper;
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;

DROP TABLE IF EXISTS u10108_pl;
DROP TABLE IF EXISTS u10108_pl_helper;

SELECT '';
set enable_wait_attached_staged_parts_to_visible = 0, enable_unique_table_attach_without_dedup = 0;
CREATE TABLE u10108_pl (d Date, k1 Int64, k2 Int64, v1 Int64) ENGINE = CnchMergeTree() PARTITION BY d ORDER BY k1 UNIQUE KEY (k1, k2);

SYSTEM STOP MERGES u10108_pl;
INSERT INTO u10108_pl SELECT '2021-01-01', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-01', 2, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-02', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-02', 2, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-03', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-03', 2, number, number FROM system.numbers LIMIT 100;
SELECT partition, count() FROM system.cnch_staged_parts where database = currentDatabase(0) and table = 'u10108_pl' and to_publish group by partition order by partition;
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;

SYSTEM STOP DEDUP WORKER u10108_pl;
ALTER TABLE u10108_pl DETACH PARTITION ID '20210101';
ALTER TABLE u10108_pl DETACH PARTITION ID '20210102';
ALTER TABLE u10108_pl DETACH PARTITION ID '20210103';
ALTER TABLE u10108_pl ATTACH PARTITION ID '20210101';
ALTER TABLE u10108_pl ATTACH PARTITION ID '20210102';
ALTER TABLE u10108_pl ATTACH PARTITION ID '20210103';
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;
SELECT partition, count() FROM system.cnch_staged_parts where database = currentDatabase(0) and table = 'u10108_pl' and to_publish group by partition order by partition;

SELECT 'test detach staged partition: ';
ALTER TABLE u10108_pl DETACH STAGED PARTITION ID '20210101';
SELECT partition, count() FROM system.cnch_staged_parts where database = currentDatabase(0) and table = 'u10108_pl' and to_publish group by partition order by partition;
ALTER TABLE u10108_pl ATTACH PARTITION ID '20210101';
SELECT partition, count() FROM system.cnch_staged_parts where database = currentDatabase(0) and table = 'u10108_pl' and to_publish group by partition order by partition;

SELECT 'test drop staged partition: ';
ALTER TABLE u10108_pl DROP STAGED PARTITION ID '20210101';
SELECT partition, count() FROM system.cnch_staged_parts where database = currentDatabase(0) and table = 'u10108_pl' and to_publish group by partition order by partition;
ALTER TABLE u10108_pl ATTACH PARTITION ID '20210101';
SELECT partition, count() FROM system.cnch_staged_parts where database = currentDatabase(0) and table = 'u10108_pl' and to_publish group by partition order by partition;

SELECT 'start dedup worker and all staged parts to be visible';
SYSTEM START DEDUP WORKER u10108_pl;
SYSTEM SYNC DEDUP WORKER u10108_pl;
SELECT d, k1, count(1), sum(v1) FROM u10108_pl GROUP BY d, k1 ORDER BY d, k1;

DROP TABLE IF EXISTS u10108_pl;

SELECT '';
SELECT 'test detach/drop staged parts with TTL';
set enable_wait_attached_staged_parts_to_visible = 0, enable_unique_table_attach_without_dedup = 0, enable_staging_area_for_write = 1;
CREATE TABLE u10108_pl (d Date, k1 Int64, k2 Int64, v1 Int64) ENGINE = CnchMergeTree() PARTITION BY d ORDER BY k1 UNIQUE KEY (k1, k2) TTL d + INTERVAL 2 DAY;

SYSTEM STOP DEDUP WORKER u10108_pl;
INSERT INTO u10108_pl SELECT '2021-01-01', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-01', 2, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-02', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-02', 2, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-03', 1, number, number FROM system.numbers LIMIT 100;
INSERT INTO u10108_pl SELECT '2021-01-03', 2, number, number FROM system.numbers LIMIT 100;
SELECT partition, count() FROM system.cnch_staged_parts where database = currentDatabase(0) and table = 'u10108_pl' and to_publish group by partition order by partition;

SELECT 'test detach staged partition: ';
ALTER TABLE u10108_pl DETACH STAGED PARTITION ID '20210101';
SELECT partition, count() FROM system.cnch_staged_parts where database = currentDatabase(0) and table = 'u10108_pl' and to_publish group by partition order by partition;
ALTER TABLE u10108_pl ATTACH PARTITION ID '20210101';
SELECT partition, count() FROM system.cnch_staged_parts where database = currentDatabase(0) and table = 'u10108_pl' and to_publish group by partition order by partition;

SELECT 'test drop staged partition: ';
ALTER TABLE u10108_pl DROP STAGED PARTITION ID '20210101';
SELECT partition, count() FROM system.cnch_staged_parts where database = currentDatabase(0) and table = 'u10108_pl' and to_publish group by partition order by partition;
ALTER TABLE u10108_pl ATTACH PARTITION ID '20210101';
SELECT partition, count() FROM system.cnch_staged_parts where database = currentDatabase(0) and table = 'u10108_pl' and to_publish group by partition order by partition;

DROP TABLE IF EXISTS u10108_pl;
