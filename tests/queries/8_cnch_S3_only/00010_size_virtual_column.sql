SELECT * FROM numbers(10) INTO OUTFILE 's3://cnch/test_00010/clickhouse_outfile_1.csv' FORMAT CSV SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000', outfile_in_server_with_tcp = 1;

SELECT * FROM numbers(1000) INTO OUTFILE 's3://cnch/test_00010/clickhouse_outfile_2.csv' FORMAT CSV SETTINGS s3_ak_id = 'minio', s3_ak_secret = 'minio123', s3_region = 'cn-beijing', s3_endpoint = 'http://minio:9000', outfile_in_server_with_tcp = 1;

SELECT count() FROM CnchS3('http://minio:9000/cnch/test_00010/*.csv', 'number UInt64', 'CSV', 'none', 'minio', 'minio123');

SELECT count() FROM CnchS3('http://minio:9000/cnch/test_00010/*.csv', 'number UInt64', 'CSV', 'none', 'minio', 'minio123') where _size > 1000;

SELECT count() FROM CnchS3('http://minio:9000/cnch/test_00010/*.csv', 'number UInt64', 'CSV', 'none', 'minio', 'minio123') where _size < 1000;

CREATE TABLE s3_table(`number` UInt64) ENGINE = CnchS3('http://minio:9000/cnch/test_00010/*.csv', 'CSV', 'none', 'minio', 'minio123');

SELECT count() FROM s3_table;

SELECT count() FROM s3_table where _size > 1000;

SELECT count() FROM s3_table where _size < 1000;
