DROP TABLE IF EXISTS test.hive_type_char_test;

CREATE  TABLE test.hive_type_char_test
(
    id Nullable(Bigint),
    name Nullable(String),
    ch Nullable(FixedString(10)),
    date String
)
ENGINE = CnchHive(`thrift://10.112.121.82:9301`, `cnchhive_ci`, `hive_type_char_test`)
PARTITION BY (date);

SELECT * FROM test.hive_type_char_test ORDER BY id, name;

DROP TABLE IF EXISTS test.hive_type_char_test;
