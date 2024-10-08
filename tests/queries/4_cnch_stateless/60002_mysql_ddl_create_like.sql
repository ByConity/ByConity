set dialect_type='MYSQL';

DROP TABLE IF EXISTS mysql_create_like_ddl1;
DROP TABLE IF EXISTS mysql_create_like_ddl2;

CREATE TABLE mysql_create_like_ddl1
(
    `id` Int32 NULL,
    `val1` timestamp NOT NULL COMMENT '中文',
    `val2` varchar NOT NULL DEFAULT 'a',
    CLUSTERED KEY(id, val1, val2),
    PRIMARY KEY(id)
)
ENGINE = 'XUANWU'
PARTITION BY VALUE((toString(val1), id))
STORAGE_POLICY = 'MIXED'
hot_partition_count = 10
BLOCK_SIZE=4096
RT_ENGINE='COLUMNSTORE'
TABLE_PROPERTIES = '{"format":"columnstore"}'
TTL toDateTime(val1) + INTERVAL 1 DAY
COMMENT 'a';

CREATE TABLE mysql_create_like_ddl2 like mysql_create_like_ddl1;
show create table mysql_create_like_ddl2;
DROP TABLE IF EXISTS mysql_create_like_ddl1;
DROP TABLE IF EXISTS mysql_create_like_ddl2;
