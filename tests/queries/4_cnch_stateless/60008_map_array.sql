CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.hive_external_table_5;
CREATE  TABLE test.hive_external_table_5
(
    id Nullable(Bigint),
    value Array(String),
    zset Map(String, double),
    date String
)
ENGINE = CnchHive(`data.olap.cnch_hms.service.lf`, `cnchhive_ci`, `hive_external_table_5`)
PARTITION BY (date);

SELECT * FROM test.hive_external_table_5 where date > '20211013' and date < '20211016' ORDER BY id, value;

SELECT zset{'sdbfr'} from test.hive_external_table_5 order by id, value;

DROP TABLE IF EXISTS test.hive_external_table_5;
