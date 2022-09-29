DROP TABLE IF EXISTS test.hive_external_table_3;
CREATE TABLE test.hive_external_table_3
(
    app_id Bigint,
    action_type Nullable(String),
    commodity_id int,
    date String,
    live_id Bigint,
    app_name String
)
ENGINE = CnchHive(`data.olap.cnch_hms.service.lf`, `cnchhive_ci`, `hive_external_table_3`)
PARTITION BY (date, live_id, app_name);

SELECT sum(app_id), count(*), avg(commodity_id) FROM test.hive_external_table_3;

DROP TABLE IF EXISTS test.hive_external_table_3;
