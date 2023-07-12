CREATE DATABASE IF NOT EXISTS csv_db;

CREATE EXTERNAL TABLE IF NOT EXISTS csv_db.simple_csv_tbl (
    `tinyint_col` tinyint,
    `smallint_col` smallint,
    `int_col` int,
    `bigint_col` bigint,
    `boolean_col` boolean,
    `float_col` float,
    `double_col` double,
    `string_col` string,
    `binary_col` binary,
    `timestamp_col` timestamp,
    `decimal_col` decimal(12,4),
    `char_col` char(50),
    `varchar_col` varchar(50),
    `date_col` date,
    `list_double_col` array<double>,
    `list_string_col` array<string>)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ( 
    'field.delim'='|', 
    'serialization.format'='|') 
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '/user/byconity/csv/simple_csv_tbl';

CREATE DATABASE IF NOT EXISTS par_db;
DROP TABLE IF EXISTS par_db.par_tbl;
CREATE TABLE IF NOT EXISTS par_db.par_tbl (
    `name` string,
    `value` int)
PARTITIONED BY (
    `event_date` string
)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    '/user/byconity/par/par_tbl';

INSERT INTO par_db.par_tbl partition(event_date='20220202') SELECT string_col, int_col FROM csv_db.simple_csv_tbl;
INSERT INTO par_db.par_tbl partition(event_date='20220203') SELECT string_col, int_col FROM csv_db.simple_csv_tbl;


CREATE DATABASE IF NOT EXISTS orc_db;
DROP TABLE IF EXISTS orc_db.orc_tbl;
CREATE TABLE IF NOT EXISTS orc_db.orc_tbl (
    `name` string,
    `value` int)
PARTITIONED BY (
    `event_date` string
)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
    '/user/byconity/orc/orc_tbl';


INSERT INTO orc_db.orc_tbl partition(event_date='20220202') SELECT string_col, int_col FROM csv_db.simple_csv_tbl;
INSERT INTO orc_db.orc_tbl partition(event_date='20220203') SELECT string_col, int_col FROM csv_db.simple_csv_tbl;

CREATE DATABASE IF NOT EXISTS str_test_db;
DROP TABLE IF EXISTS str_test_db.fixed_str;
CREATE TABLE IF NOT EXISTS str_test_db.fixed_str (
    `fixed_str` char(16),
    `varchar_str` varchar(10),
    `str` string )
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    '/user/byconity/str_test_db/fixed_str';

INSERT INTO str_test_db.fixed_str values('1234567890123456', 'abc', 'abc'), ('2345678901234567','def','def')