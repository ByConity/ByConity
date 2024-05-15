SET dialect_type = 'MYSQL';

DROP TABLE IF EXISTS key_use_mysql_func;

CREATE TABLE key_use_mysql_func
(
    `id` int,
    `datetime` DateTime,
    PRIMARY KEY(id)
)ENGINE = 'XUANWU'
PARTITION BY VALUE(DATE_FORMAT(datetime, '%Y%m%d'))
DISTRIBUTED BY HASH(id)
STORAGE_POLICY = 'MIXED'
hot_partition_count = 10
BLOCK_SIZE=4096
RT_ENGINE='COLUMNSTORE';

INSERT INTO key_use_mysql_func VALUES (1, '2024-01-01 10:00:00');

SELECT _partition_value FROM key_use_mysql_func;
