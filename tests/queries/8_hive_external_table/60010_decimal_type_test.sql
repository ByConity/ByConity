DROP TABLE IF EXISTS test.hive_type_dec_test;

CREATE TABLE test.hive_type_dec_test
(
    id Nullable(Bigint),
    dec1 Nullable(Decimal(10,0)),
    dec2 Nullable(Decimal(38,18)),
    date String
)
ENGINE = CnchHive(`thrift://10.112.121.82:9301`, `cnchhive_ci`, `hive_type_dec_test`)
PARTITION BY (date);

SELECT dec1, count(dec1), dec2, count(dec2) FROM test.hive_type_dec_test GROUP BY dec1, dec2 ORDER BY dec1;

DROP TABLE IF EXISTS test.hive_type_dec_test;
