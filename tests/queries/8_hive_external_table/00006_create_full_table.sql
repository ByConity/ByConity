DROP TABLE IF EXISTS test.hive_external_table_3;
CREATE TABLE test.hive_external_table_3
(
    app_id Nullable(Bigint),
    action_type Nullable(String),
    commodity_id Nullable(int),
    device_id Nullable(String),
    platfrom_name Nullable(String),
    date String,
    live_id Bigint,
    app_name String
)
ENGINE = CnchHive(`data.olap.cnch_hms.service.lf`, `cnchhive_ci`, `hive_external_table_3`)
PARTITION BY (date, live_id, app_name);

SELECT *  FROM test.hive_external_table_3 order by app_id, commodity_id, app_name;

DROP TABLE IF EXISTS test.hive_external_table_3;
