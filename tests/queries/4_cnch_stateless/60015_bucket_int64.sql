DROP TABLE IF EXISTS test.bucket_int64_test;

CREATE TABLE test.bucket_int64_test
(
    id Bigint,
    name String,
    device_id Bigint,
    p Bigint
)ENGINE = CnchHive(`thrift://10.112.121.82:9301`,`cnch_hive_external_table`,`bucket_int64_test`)
PARTITION BY (p)
CLUSTER BY device_id INTO 5 BUCKETS;

select * from test.bucket_int64_test where p = 1 AND device_id = 4209368873053927;

DROP TABLE IF EXISTS test.bucket_int64_test;
