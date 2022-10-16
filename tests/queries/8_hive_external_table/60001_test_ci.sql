DROP TABLE IF EXISTS test.test_ci;

CREATE TABLE test.test_ci
(
    `name` String,
    `id` Bigint
)
ENGINE = CnchHive(`thrift://10.112.121.82:9301`, `cnchhive_ci_test_1`, `ci_test`)
PARTITION BY id; 

SELECT * FROM test.test_ci ORDER BY name;

DROP TABLE IF EXISTS test.test_ci;
