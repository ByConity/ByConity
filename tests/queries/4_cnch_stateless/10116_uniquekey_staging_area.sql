set enable_staging_area_for_write = 1;

SELECT '# partition level unique';
DROP TABLE IF EXISTS u10116_pl;

CREATE TABLE u10116_pl (d Date, id Int32, s String) ENGINE = CnchMergeTree()
PARTITION BY d ORDER BY s UNIQUE KEY id SETTINGS partition_level_unique_keys = 1;

SYSTEM START DEDUP WORKER u10116_pl;
SYSTEM STOP DEDUP WORKER u10116_pl;

INSERT INTO u10116_pl VALUES ('2021-03-01', 1, '1a'), ('2021-03-01', 2, '2a'), ('2021-03-01', 3, '3a'), ('2021-03-01', 1, '1b'), ('2021-04-01', 1, '1a'), ('2021-03-01', 1, '1c'), ('2021-03-01', 3, '3b');
INSERT INTO u10116_pl VALUES ('2021-03-01', 0, '0a'), ('2021-03-01', 1, '1d'), ('2021-03-01', 1, '1e'), ('2021-03-01', 4, '4a'), ('2021-04-01', 1, '1b');

SYSTEM START DEDUP WORKER u10116_pl;
SYSTEM SYNC DEDUP WORKER u10116_pl;
SELECT 'after 1st dedup round';
SELECT * FROM u10116_pl ORDER BY d, id;
SYSTEM STOP DEDUP WORKER u10116_pl;

INSERT INTO u10116_pl VALUES ('2021-03-01', 2, '2b'), ('2021-03-01', 4, '4b');
INSERT INTO u10116_pl VALUES ('2021-03-01', 5, '5a'), ('2021-03-01', 6, '6a');

SYSTEM START DEDUP WORKER u10116_pl;
SYSTEM SYNC DEDUP WORKER u10116_pl;
SELECT 'after 2nd dedup round';
SELECT * FROM u10116_pl ORDER BY d, id;

SELECT 'after 1st repair';
SYSTEM DEDUP u10116_pl PARTITION '20210301' FOR REPAIR;
SELECT * FROM u10116_pl ORDER BY d, id;

SELECT 'after 2nd repair';
SYSTEM DEDUP u10116_pl FOR REPAIR;
SELECT * FROM u10116_pl ORDER BY d, id;

DROP TABLE IF EXISTS u10116_pl;

----------------------------
SELECT '# table level unique';
DROP TABLE IF EXISTS u10116_tl;

CREATE TABLE u10116_tl (d Date, id Int32, s String) ENGINE = CnchMergeTree()
PARTITION BY d ORDER BY s UNIQUE KEY id SETTINGS partition_level_unique_keys = 0;

SYSTEM START DEDUP WORKER u10116_tl;
SYSTEM STOP DEDUP WORKER u10116_tl;

INSERT INTO u10116_tl Format Values SETTINGS enable_staging_area_for_write = 1
('2021-03-01', 1, '1a'), ('2021-03-01', 2, '2a'), ('2021-03-01', 3, '3a');

INSERT INTO u10116_tl Format Values SETTINGS enable_staging_area_for_write = 1
('2021-02-01', 1, '1b'), ('2021-03-01', 2, '2b'), ('2021-04-01', 3, '3b');

SYSTEM START DEDUP WORKER u10116_tl;

SYSTEM SYNC DEDUP WORKER u10116_tl;
SELECT * FROM u10116_tl ORDER BY id;

SELECT 'after repair';
SYSTEM DEDUP u10116_tl FOR REPAIR;
SELECT * FROM u10116_tl ORDER BY id;


DROP TABLE IF EXISTS u10116_tl;
