DROP TABLE IF EXISTS test_ci;

CREATE TABLE test_ci
(
    `name` String,
    `id` Bigint
)
ENGINE = CnchHive(`data.olap.cnch_hms.service.lf`, `cnchhive_ci_test_1`, `ci_test`)
PARTITION BY id; 

SELECT * FROM test_ci ORDER BY name;

DROP TABLE IF EXISTS test_ci;
