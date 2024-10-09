SET s3_max_single_part_upload_size = 5242880;
SET s3_min_upload_part_size = 5242880;
SET s3_use_parallel_upload = 1;

DROP TABLE IF EXISTS verify_table;
Create TABLE verify_table (a UInt64) ENGINE = CnchMergeTree() ORDER BY a;

SELECT * FROM numbers(1000000) INTO OUTFILE 's3://cnch/test_00002/single_file.csv' FORMAT CSV SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000', outfile_in_server_with_tcp = 1;
INSERT INTO verify_table FORMAT CSV INFILE 's3://cnch/test_00002/single_file.csv' SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000';
SELECT count() FROM verify_table;

SELECT * FROM numbers(1000000) INTO OUTFILE 's3://cnch/test_00002_directory/' FORMAT CSV SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000', outfile_in_server_with_tcp = 1;
INSERT INTO verify_table FORMAT CSV INFILE 's3://cnch/test_00002_directory/*.csv' SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000';
SELECT count() FROM verify_table;

SELECT * FROM verify_table INTO OUTFILE 's3://cnch/test_00002_distributed/' FORMAT CSV SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000', enable_optimizer= 1, enable_distributed_output = 1;
INSERT INTO verify_table FORMAT CSV INFILE 's3://cnch/test_00002_distributed/*.csv' SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000';
SELECT count() FROM verify_table;
