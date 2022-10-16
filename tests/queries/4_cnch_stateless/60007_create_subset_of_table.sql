DROP TABLE IF EXISTS test.hive_external_table_3_05;
CREATE TABLE test.hive_external_table_3_05
(
    app_id Nullable(Bigint),
    date String,
    live_id Bigint,
    app_name String
)
ENGINE = CnchHive(`thrift://10.112.121.82:9301`, `cnchhive_ci`, `hive_external_table_3`)
PARTITION BY (date, live_id, app_name);

SELECT *  FROM test.hive_external_table_3_05 order by app_id, app_name;

DROP TABLE IF EXISTS test.hive_external_table_3_05;
