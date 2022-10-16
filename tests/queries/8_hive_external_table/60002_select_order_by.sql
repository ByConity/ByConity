DROP TABLE IF EXISTS test.hive_external_table_3_01;
CREATE TABLE test.hive_external_table_3_01
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

SELECT * FROM test.hive_external_table_3_01 where date >= '20211013' order by app_id, commodity_id, app_name;

SELECT action_type AS Carrier, avg(app_id) AS c3 FROM test.hive_external_table_3_01 WHERE date >= '20211013' AND date <= '20211016' GROUP BY Carrier ORDER BY c3 DESC;

SELECT app_id, count(*) AS c FROM test.hive_external_table_3_01 WHERE date != '20211013' GROUP BY app_id ORDER BY c  DESC, app_id asc;

SELECT count(*) FROM test.hive_external_table_3_01 WHERE app_id != 0;

DROP TABLE IF EXISTS test.hive_external_table_3_01;
