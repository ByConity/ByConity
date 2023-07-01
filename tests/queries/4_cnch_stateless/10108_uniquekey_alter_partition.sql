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
