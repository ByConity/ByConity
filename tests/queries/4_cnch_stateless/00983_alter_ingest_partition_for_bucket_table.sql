DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;

CREATE TABLE target_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY c2 INTO 3 BUCKETS ORDER BY id;
CREATE TABLE source_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY c2 INTO 3 BUCKETS ORDER BY id;

SYSTEM START MERGES target_bucket_table;

INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'a', 'b' from numbers(2);
INSERT INTO target_bucket_table SELECT '2021-01-01', number, 'c', 'd' from numbers(2,2);

INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'e', 'f' from numbers(3);
INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'g', 'h' from numbers(3,2);

SELECT * FROM target_bucket_table ORDER BY id;


ALTER TABLE target_bucket_table ingest partition '2021-01-01' columns c1 key id from source_bucket_table;


SELECT * FROM target_bucket_table ORDER BY id;

DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;

-- Test the case when target table has no part 
CREATE TABLE target_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY c2 INTO 3 BUCKETS ORDER BY id;
CREATE TABLE source_bucket_table (`p_date` Date, `id` Int32, `c1` String, `c2` String) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY c2 INTO 3 BUCKETS ORDER BY id;
SYSTEM START MERGES target_bucket_table;
INSERT INTO source_bucket_table SELECT '2021-01-01', number, 'e', 'f' from numbers(3);
SYSTEM START MERGES target_bucket_table;
ALTER TABLE target_bucket_table ingest partition '2021-01-01' columns c1 key id from source_bucket_table;
SELECT '---';
SELECT * FROM source_bucket_table ORDER BY id;
SELECT '---';
SELECT * FROM target_bucket_table ORDER BY id;
SELECT '---';
DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;

-- Test with map and with query doesn't specify key

CREATE TABLE target_bucket_table (`p_date` Date, `id` Int32, `cluster_key` String, `name` Map(String, String)) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY cluster_key INTO 3 BUCKETS ORDER BY id; 
CREATE TABLE source_bucket_table (`p_date` Date, `id` Int32, `cluster_key` String, `name` Map(String, String)) ENGINE = CnchMergeTree PARTITION BY p_date CLUSTER BY cluster_key INTO 3 BUCKETS ORDER BY id; 

SYSTEM START MERGES target_bucket_table;
INSERT INTO source_bucket_table VALUES ('2020-01-01', 1, 'ck1', {'key1': 'val1', 'key2': 'val2', 'key3': 'val3'}), ('2020-01-01', 2, 'ck2', {'key4': 'val4', 'key5': 'val5', 'key6': 'val6'}), ('2020-01-01', 3, 'ck3', {'key7': 'val7', 'key8': 'val8', 'key9': 'val9'}), ('2020-01-01', 4, 'ck4',{'key10': 'val10', 'key11': 'val11', 'key12': 'val12'});

INSERT INTO target_bucket_table VALUES ('2020-01-01', 1, 'ck5', {'key13': 'val13'});
ALTER TABLE target_bucket_table INGEST PARTITION '2020-01-01'  COLUMNS name{'key1'}, name{'key2'}, name{'key3'}, name{'key4'}, name{'key5'}, name{'key6'}, name{'key7'}, name{'key8'}, name{'key9'}, name{'key10'} FROM source_bucket_table;
SELECT * FROM target_bucket_table ORDER BY id;

DROP TABLE IF EXISTS target_bucket_table;
DROP TABLE IF EXISTS source_bucket_table;
