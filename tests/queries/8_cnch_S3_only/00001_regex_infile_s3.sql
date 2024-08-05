SELECT '1' INTO OUTFILE 's3://cnch/test_00001/clickhouse_outfile_1.csv' FORMAT CSV SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000', outfile_in_server_with_tcp = 1;

SELECT '2' INTO OUTFILE 's3://cnch/test_00001/clickhouse_outfile_2.csv' FORMAT CSV SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000', outfile_in_server_with_tcp = 1;

DROP TABLE IF EXISTS test_regex_infile;

Create TABLE test_regex_infile (a UInt8) ENGINE = CnchMergeTree() ORDER BY a;

INSERT INTO test_regex_infile FORMAT CSV INFILE 's3://cnch/test_00001/*' SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000';

SELECT count() FROM test_regex_infile;

INSERT INTO test_regex_infile FORMAT CSV INFILE 's3://cnch/test_00001/clickhouse_outfile_?.csv' SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000';

SELECT count() FROM test_regex_infile;
