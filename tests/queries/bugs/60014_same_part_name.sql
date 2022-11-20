CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS test.test_ci;

CREATE TABLE test.test_ci
(
    `name` String,
    `id` Bigint
)
ENGINE = CnchHive(`data.olap.cnch_hms.service.lf`, `cnch_hive_external_table`, `ci_test`)
PARTITION BY id;

-- table test_ci in different partition exist same part name
-- maybe this sql cannot fullly explain this issue
-- TODO: use system table show the same part
SELECT * FROM test.test_ci ORDER BY name;

DROP TABLE IF EXISTS test.test_ci;
