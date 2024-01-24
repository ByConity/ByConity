DROP TABLE IF EXISTS test_ci;

CREATE TABLE test_ci
(
    `name` String,
    `id` Bigint
)
ENGINE = CnchHive(`data.olap.cnch_hms.service.lf`, `cnch_hive_external_table`, `ci_test`)
PARTITION BY id
SETTINGS cnch_server_vw = 'server_vw_default';

ALTER TABLE test_ci modify setting cnch_server_vw = 'server_vw_default';

SELECT * FROM test_ci ORDER BY name;

DROP TABLE IF EXISTS test_ci;
