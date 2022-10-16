DROP TABLE IF EXISTS test.hive_bucket_test;
CREATE TABLE test.hive_bucket_test
(
    id int,
    name String,
    date String
)ENGINE = CnchHive(`thrift://10.112.121.82:9301`, `test_tiger`, `hive_bucket_test`)
PARTITION BY (date)
CLUSTER BY id INTO 4 BUCKETS
ORDER BY name;

/* id = 1 will purn useless parts */
SELECT * FROM test.hive_bucket_test WHERE id = 1 ORDER BY name;

/* this case cann't purn parts */
SELECT * FROM test.hive_bucket_test WHERE id >= 1 AND id <= 3 ORDER BY name;

DROP TABLE IF EXISTS test.hive_bucket_test;
