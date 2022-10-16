DROP TABLE IF EXISTS test.hive_external_table_3_02;
CREATE TABLE test.hive_external_table_3_02
(
    app_id Bigint,
    action_type Nullable(String),
    commodity_id int,
    date String,
    live_id Bigint,
    app_name String
)
ENGINE = CnchHive(`thrift://10.112.121.82:9301`, `cnchhive_ci`, `hive_external_table_3`)
PARTITION BY (date, live_id, app_name);

SELECT DISTINCT action_type FROM test.hive_external_table_3_02 order by action_type;

DROP TABLE IF EXISTS test.hive_external_table_3_02;
