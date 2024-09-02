SET max_ingest_task_on_workers = 0;

SELECT '---ingest with inefficient_ingest_partition---';

DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;

CREATE TABLE target_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY id INTO 3 BUCKETS ORDER BY id;
CREATE TABLE source_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY id INTO 3 BUCKETS ORDER BY id;

SYSTEM START MERGES target_bucket_table;

INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'a', 'b' from numbers(2);
INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'c', 'd' from numbers(2,2);

INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'e', 'f' from numbers(3);
INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'g', 'h' from numbers(3,20);

SELECT * FROM target_bucket_table ORDER BY id;

ALTER TABLE target_bucket_table ingest partition '2021-01-01' columns c1 key id from source_bucket_table SETTINGS enable_memory_efficient_ingest_partition=0;

SELECT '---result---';

SELECT * FROM target_bucket_table ORDER BY id;


SELECT '---ingest with enfficient_ingest_partition---';

DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;

CREATE TABLE target_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY id INTO 3 BUCKETS ORDER BY id;
CREATE TABLE source_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY id INTO 3 BUCKETS ORDER BY id;

SYSTEM START MERGES target_bucket_table;

INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'a', 'b' from numbers(2);
INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'c', 'd' from numbers(2,2);

INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'e', 'f' from numbers(3);
INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'g', 'h' from numbers(3,20);

SELECT * FROM target_bucket_table ORDER BY id;

ALTER TABLE target_bucket_table ingest partition '2021-01-01' columns c1 key id from source_bucket_table SETTINGS enable_memory_efficient_ingest_partition=1;

SELECT '---result---';

SELECT * FROM target_bucket_table ORDER BY id;


SELECT '--- ingest with mulit key table 1 ---';

DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;


CREATE TABLE target_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String, `id_str` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY (id, id_str) INTO 3 BUCKETS ORDER BY (id, id_str);
CREATE TABLE source_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String, `id_str` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY (id, id_str) INTO 3 BUCKETS ORDER BY (id, id_str);

SYSTEM START MERGES target_bucket_table;

INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'a', 'b', toString(number) from numbers(2);
INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'c', 'd', toString(number) from numbers(2,2);

INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'e', 'f', toString(number) from numbers(3);
INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'g', 'h', toString(number) from numbers(3,10);

SELECT * FROM target_bucket_table ORDER BY id;

ALTER TABLE target_bucket_table ingest partition '2021-01-01' columns c1, c2 key id, id_str from source_bucket_table SETTINGS enable_memory_efficient_ingest_partition=1;

SELECT '---result---';

SELECT * FROM target_bucket_table ORDER BY id;

SELECT '--- ingest with mulit key table 2 ---';

DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;

CREATE TABLE target_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String, `id_str` String, `id10` Int32) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY (id, id_str) INTO 3 BUCKETS ORDER BY (id, id_str, id10);
CREATE TABLE source_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String, `id_str` String, `id10` Int32) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY (id, id_str) INTO 3 BUCKETS ORDER BY (id, id_str, id10);

SYSTEM START MERGES target_bucket_table;

INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'a', 'b', toString(number), number * 10 from numbers(2);
INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'c', 'd', toString(number), number * 10 from numbers(2,2);

INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'e', 'f', toString(number), number * 10 from numbers(3);
INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'g', 'h', toString(number), number * 10 from numbers(3,10);

SELECT * FROM target_bucket_table ORDER BY id;

ALTER TABLE target_bucket_table ingest partition '2021-01-01' columns c1, c2 key id, id_str, id10 from source_bucket_table SETTINGS enable_memory_efficient_ingest_partition=1;

SELECT '---result---';

SELECT * FROM target_bucket_table ORDER BY id;

SELECT '---ingest with selected buckets---';

DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;

CREATE TABLE target_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY id INTO 3 BUCKETS ORDER BY id;
CREATE TABLE source_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY id INTO 3 BUCKETS ORDER BY id;

SYSTEM START MERGES target_bucket_table;

INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'a', 'b' from numbers(2);
INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'c', 'd' from numbers(2,2);

INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'e', 'f' from numbers(3);
INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'g', 'h' from numbers(3,20);

SELECT * FROM target_bucket_table ORDER BY id;

ALTER TABLE target_bucket_table ingest partition '2021-01-01' columns c1 key id from source_bucket_table BUCKETS 0 SETTINGS enable_memory_efficient_ingest_partition=0;

SELECT '---result---';

SELECT * FROM target_bucket_table ORDER BY id;

DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;

SELECT '---ingest with selected multiple buckets---';

CREATE TABLE target_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY id INTO 3 BUCKETS ORDER BY id;
CREATE TABLE source_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY id INTO 3 BUCKETS ORDER BY id;

SYSTEM START MERGES target_bucket_table;

INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'a', 'b' from numbers(2);
INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'c', 'd' from numbers(2,2);

INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'e', 'f' from numbers(3);
INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'g', 'h' from numbers(3,20);

SELECT * FROM target_bucket_table ORDER BY id;

ALTER TABLE target_bucket_table ingest partition '2021-01-01' columns c1 key id from source_bucket_table BUCKETS 0, 1, 2 SETTINGS enable_memory_efficient_ingest_partition=0;

SELECT '---result---';

SELECT * FROM target_bucket_table ORDER BY id;

DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;

SELECT '---ingest with empty key---';

CREATE TABLE target_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String, `id_str` String, `id10` Int32) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY (id) INTO 3 BUCKETS ORDER BY (id_str, id, id10);
CREATE TABLE source_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String, `id_str` String, `id10` Int32) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY (id) INTO 3 BUCKETS ORDER BY (id_str, id, id10);

SYSTEM START MERGES target_bucket_table;

INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'a', 'b', toString(number), number * 10 from numbers(2);
INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'c', 'd', toString(number), number * 10 from numbers(2,2);

INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'e', 'f', toString(number), number * 10 from numbers(3);
INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'g', 'h', toString(number), number * 10 from numbers(3,10);

SELECT * FROM target_bucket_table ORDER BY id;

ALTER TABLE target_bucket_table ingest partition '2021-01-01' columns c1, c2 from source_bucket_table ;

SELECT '---result---';

SELECT * FROM target_bucket_table ORDER BY id;

DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;
