set dialect_type='MYSQL';
use test;
DROP TABLE IF EXISTS mysql_create_ddl1;
DROP TABLE IF EXISTS mysql_create_ddl2;
DROP TABLE IF EXISTS mysql_create_ddl3;
DROP TABLE IF EXISTS mysql_create_ddl4;
DROP TABLE IF EXISTS mysql_create_ddl5;
DROP TABLE IF EXISTS mysql_create_ddl7;
DROP TABLE IF EXISTS test_create_table_unique1;
DROP TABLE IF EXISTS test_create_table_unique2;
CREATE TABLE mysql_create_ddl1
(
    `id` Int32 NULL,
    `val1` Datetime NOT NULL COMMENT '中文',
    `val2` varchar NOT NULL DEFAULT 'a',
    CLUSTERED KEY(id, val1, val2)
)
ENGINE = 'XUANWU'
PARTITION BY VALUE((toString(val1), id))
STORAGE_POLICY = 'MIXED'
hot_partition_count = 10
BLOCK_SIZE=4096
RT_ENGINE='COLUMNSTORE'
TABLE_PROPERTIES = '{"format":"columnstore"}'
TTL toDateTime(val1) + 1
COMMENT 'a';

CREATE TABLE mysql_create_ddl2
(
    `id` Int32 NULL,
    `val1` timestamp,
    `val2` varchar NOT NULL DEFAULT 'a',
    CLUSTERED KEY(id, val1, val2)
)
ENGINE = 'XUANWU'
PARTITION BY VALUE((toString(val1), id))
STORAGE_POLICY = 'MIXED'
hot_partition_count = 10
BLOCK_SIZE=8192
RT_ENGINE='COLUMNSTORE'
TABLE_PROPERTIES = '{"format":"columnstore"}'
COMMENT 'a';

CREATE TABLE mysql_create_ddl3
(
    `id` Int32 NOT NULL,
    `val1` timestamp NOT NULL COMMENT '中文',
    `val2` varchar NOT NULL DEFAULT 'a',
    PRIMARY KEY(id)
)
STORAGE_POLICY = 'MIXED'
hot_partition_count = 10
BLOCK_SIZE=4096
RT_ENGINE='COLUMNSTORE'
TABLE_PROPERTIES = '{"format":"columnstore"}'
PARTITION BY VALUE(toDate(val1))
COMMENT 'a';

CREATE TABLE mysql_create_ddl4
(
    `id` Int32 NULL,
    `val1` timestamp NOT NULL COMMENT '中文',
    `val2` varchar NOT NULL DEFAULT 'a'
);

CREATE TABLE mysql_create_ddl5
(
    `id` Int32 NOT NULL,
    `val1` timestamp NOT NULL COMMENT '中文',
    `val2` varchar NOT NULL DEFAULT 'a',
    constraint un1 unique(id)
);

describe table mysql_create_ddl1;
show create table mysql_create_ddl1;
describe table mysql_create_ddl2;
show create table mysql_create_ddl2;
describe table mysql_create_ddl3;
show create table mysql_create_ddl3;
describe table mysql_create_ddl4;
show create table mysql_create_ddl4;
describe table mysql_create_ddl5;
show create table mysql_create_ddl5;

DROP TABLE mysql_create_ddl1;
DROP TABLE mysql_create_ddl2;
DROP TABLE mysql_create_ddl3;
DROP TABLE mysql_create_ddl4;
DROP TABLE mysql_create_ddl5;

-- CREATE TABLE test_create_table_unique1
-- (
--     `int_col_1` UInt64 NOT NULL,
--     `int_col_2` Nullable(UInt64),
--     `int_col_3` LowCardinality(Int8),
--     `int_col_4` boolean,
--     `int_col_5` tinyint,
--     `int_col_6` bigint,
--     `str_col_1` String NOT NULL,
--     `str_col_2` varchar,
--     `float_col_1` Float64,
--     `float_col_2` decimal(3, 2),
--     `date_col_1` Date32,
--     `date_col_2` DateTime('Asia/Istanbul'),
--     `enum_col_1` Enum('a', 'b', 'c', 'd'),
--     `map_col_1` Map(String, String) NOT NULL,
--     `map_col_2` Map(String, UInt64) NOT NULL,
--     CLUSTERED KEY(int_col_1),
--     PRIMARY KEY(int_col_1, str_col_1)
-- )
-- ENGINE = 'XUANWU'
-- PARTITION BY VALUE((int_col_1, date_col_1))
-- DISTRIBUTED BY HASH(int_col_1)
-- STORAGE_POLICY = 'MIXED'
-- hot_partition_count = 10
-- BLOCK_SIZE = 4096
-- TABLE_PROPERTIES = '{"format":"columnstore"}'
-- TTL toDate(date_col_1) + 30; -- { serverError 538 }

CREATE TABLE test_create_table_unique2
(
    `int_col_1` UInt64 NOT NULL,
    `int_col_2` UInt64 NOT NULL,
    `int_col_3` LowCardinality(Int8),
    `int_col_4` boolean,
    `int_col_5` tinyint,
    `int_col_6` bigint,
    `str_col_1` String NOT NULL,
    `str_col_2` varchar,
    `float_col_1` Float64,
    `float_col_2` decimal(3, 2),
    `date_col_1` Date32 NOT NULL,
    `date_col_2` DateTime64,
    `map_col_1` Map(String, String) NOT NULL,
    `map_col_2` Map(String, UInt64) NOT NULL,
    CLUSTERED KEY(int_col_1),
    PRIMARY KEY(int_col_1, str_col_1)
)
ENGINE = 'XUANWU'
PARTITION BY VALUE((int_col_1, date_col_1))
STORAGE_POLICY = 'MIXED'
hot_partition_count = 10
BLOCK_SIZE = 4096
TABLE_PROPERTIES = '{"format":"columnstore"}'
TTL toDate(date_col_1) + 30;

CREATE TABLE mysql_create_ddl7
(
 `id` Int32 NOT NULL,
 `val1` timestamp NOT NULL DEFAULT now(),
 `val2` varchar NOT NULL DEFAULT 'a',
 CLUSTERED KEY(id, val1, val2),
 PRIMARY KEY(id)
)ENGINE = 'XUANWU'
PARTITION BY VALUE(toDate(val1))
STORAGE_POLICY = 'MIXED'
hot_partition_count = 10
BLOCK_SIZE=4096
RT_ENGINE='COLUMNSTORE'
TABLE_PROPERTIES = '{"format":"columnstore"}'
TTL toDateTime(val1) + 1
COMMENT 'a';

-- DROP TABLE IF EXISTS hive_table;
-- CREATE TABLE hive_table ENGINE = CnchHive('data.olap.catalogservice.service.lf', 'ecom', 'dwd_product_comment_info_df');
-- DROP TABLE IF EXISTS hive_table;

SHOW CREATE TABLE test_create_table_unique2;
SHOW CREATE TABLE mysql_create_ddl7;

DROP TABLE IF EXISTS mysql_create_ddl7;
DROP TABLE IF EXISTS test_create_table_unique1;
DROP TABLE IF EXISTS test_create_table_unique2;
