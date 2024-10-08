set dialect_type='MYSQL';

DROP TABLE IF EXISTS mysql_create_ddl1;
DROP TABLE IF EXISTS mysql_create_ddl2;
DROP TABLE IF EXISTS mysql_create_ddl3;
DROP TABLE IF EXISTS mysql_create_ddl4;
DROP TABLE IF EXISTS mysql_create_ddl5;
DROP TABLE IF EXISTS mysql_create_ddl6;
DROP TABLE IF EXISTS mysql_create_ddl7;
DROP TABLE IF EXISTS mysql_create_ddl8;
DROP TABLE IF EXISTS mysql_create_ddl9;
DROP TABLE IF EXISTS test_create_table_unique1;
DROP TABLE IF EXISTS test_create_table_unique2;
CREATE TABLE mysql_create_ddl1
(
    `id` Int32 NULL,
    `val1` Datetime NOT NULL COMMENT '中文',
    `val2` varchar NOT NULL DEFAULT 'a',
    CLUSTERED KEY(id, val1, val2)
)
PARTITION BY VALUE((toString(val1), id))
STORAGE_POLICY = 'MIXED'
hot_partition_count = 10
ENGINE = 'XUANWU'
BLOCK_SIZE=4096
RT_ENGINE='COLUMNSTORE'
TABLE_PROPERTIES = '{"format":"columnstore"}'
TTL toDateTime(val1) + INTERVAL 1 DAY
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
    `val2` varchar NOT NULL DEFAULT 'a',
    UNIQUE KEY(id) USING BTREE
);

CREATE TABLE mysql_create_ddl5
(
    `id` Int32 NOT NULL PRIMARY KEY,
    `val1` timestamp NOT NULL COMMENT '中文',
    `val2` varchar NOT NULL DEFAULT 'a',
    constraint un1 unique(id)
);

CREATE TABLE mysql_create_ddl6
(
    `id` Int32 NOT NULL,
    `id2` Decimal(20, 0),
    `val1` timestamp NOT NULL COMMENT '中文',
    `val2` varchar NOT NULL DEFAULT 'a',
    PRIMARY KEY(id, id2)
) INDEX_ALL='Y' ENGINE='OSS' BLOCK_SIZE=8192 TABLE_PROPERTIES='{"accessid":"******","skip_header_line_count":"1","endpoint":"","accesskey":"******","delimiter":";","url":""}';
set datetime_format_mysql_definition = 1;
create table mysql_create_ddl8(
    id Int,
    dt timestamp DEFAULT CURRENT_TIMESTAMP,
    dt2 timestamp(1) DEFAULT CURRENT_TIMESTAMP,
    dt3 timestamp('UTC') DEFAULT CURRENT_TIMESTAMP(),
    dt4 datetime64(2) DEFAULT CURRENT_TIMESTAMP(2),
    dt5 datetime(3) DEFAULT CURRENT_TIMESTAMP(8),
    dt6 datetime(4) DEFAULT CURRENT_TIMESTAMP(4),
    dt7 datetime(5) DEFAULT CURRENT_TIMESTAMP(8),
    dt8 datetime(6) DEFAULT CURRENT_TIMESTAMP(2),
    dt9 datetime(7) DEFAULT CURRENT_TIMESTAMP(9),
    dt10 datetime(8) DEFAULT CURRENT_TIMESTAMP(4),
    dt11 datetime(9) DEFAULT CURRENT_TIMESTAMP(5),
    primary key(id, dt, dt2)
);
-- not verify results since now() is changing
-- timestamp(8) DEFAULT CURRENT_TIMESTAMP throws error in mysql
insert into mysql_create_ddl8(id) values (1);
set datetime_format_mysql_definition = 0;
create table mysql_create_ddl9(id Int, dt timestamp, dt2 timestamp(8), dt3 timestamp('UTC'), dt4 datetime64(2), dt5 datetime(5), dt6 datetime,  primary key(id, dt, dt2));
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
describe table mysql_create_ddl6;
show create table mysql_create_ddl6;
describe table mysql_create_ddl8;
show create table mysql_create_ddl8;
describe table mysql_create_ddl9;
show create table mysql_create_ddl9;

DROP TABLE mysql_create_ddl1;
DROP TABLE mysql_create_ddl2;
DROP TABLE mysql_create_ddl3;
DROP TABLE mysql_create_ddl4;
DROP TABLE mysql_create_ddl5;
DROP TABLE mysql_create_ddl6;
DROP TABLE mysql_create_ddl8;

CREATE TABLE test_create_table_unique1
(
    `int_col_1` UInt64 NOT NULL,
    `int_col_2` Nullable(UInt64),
    `int_col_3` LowCardinality(Int8),
    `int_col_4` boolean,
    `int_col_5` tinyint,
    `int_col_6` bigint,
    `str_col_1` String NOT NULL,
    `str_col_2` varchar,
    `float_col_1` Float64,
    `float_col_2` decimal(3, 2),
    `date_col_1` Date32,
    `date_col_2` DateTime('Asia/Istanbul'),
    `enum_col_1` Enum('a', 'b', 'c', 'd'),
    `map_col_1` Map(String, String) NOT NULL,
    `map_col_2` Map(String, UInt64) NOT NULL,
    CLUSTERED KEY(int_col_1),
    PRIMARY KEY(int_col_1, str_col_1)
)
ENGINE = 'XUANWU'
PARTITION BY VALUE((int_col_1, date_col_1))
DISTRIBUTED BY HASH(int_col_1)
STORAGE_POLICY = 'MIXED'
hot_partition_count = 10
BLOCK_SIZE = 4096
TABLE_PROPERTIES = '{"format":"columnstore"}'
TTL toDate(date_col_1) + INTERVAL 30 DAY;

set enable_bucket_for_distribute=0;

DROP TABLE test_create_table_unique1;
CREATE TABLE test_create_table_unique1
(
    `int_col_1` UInt64 NOT NULL,
    `int_col_2` Nullable(UInt64),
    `int_col_3` LowCardinality(Int8),
    `int_col_4` boolean,
    `int_col_5` tinyint,
    `int_col_6` bigint,
    `str_col_1` String NOT NULL,
    `str_col_2` varchar,
    `float_col_1` Float64,
    `float_col_2` decimal(3, 2),
    `date_col_1` Date32,
    `date_col_2` DateTime('Asia/Istanbul'),
    `enum_col_1` Enum('a', 'b', 'c', 'd'),
    `map_col_1` Map(String, String) NOT NULL,
    `map_col_2` Map(String, UInt64) NOT NULL,
    CLUSTERED KEY(int_col_1),
    PRIMARY KEY(int_col_1, str_col_1)
)
ENGINE = 'XUANWU'
PARTITION BY VALUE((int_col_1, date_col_1))
DISTRIBUTED BY HASH(int_col_1)
STORAGE_POLICY = 'MIXED'
hot_partition_count = 10
BLOCK_SIZE = 4096
TABLE_PROPERTIES = '{"format":"columnstore"}'
TTL toDate(date_col_1) + INTERVAL 30 DAY;
SHOW CREATE TABLE test_create_table_unique1;

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
TTL toDate(date_col_1) + INTERVAL 30 DAY;

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
TTL toDateTime(val1) + INTERVAL 1 DAY
COMMENT 'a';

DROP TABLE IF EXISTS record_60000;
DROP TABLE IF EXISTS test_bits_60000;
CREATE TABLE `record_60000` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `uid` bigint(20) DEFAULT NULL,
  `room_uid` bigint(20) DEFAULT NULL,
  `event_type` tinyint(4) DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  `source` tinyint(4) DEFAULT NULL,
  `idd` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_create_time` (`create_time`),
  KEY `idx_uid` (`uid`),
  KEY `idx_room_uid` (`room_uid`,`source`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=75253185 DEFAULT CHARSET=utf8;

CREATE TABLE test_bits_60000 (a Int32, b bit(1));
INSERT INTO test_bits_60000 VALUES (0, 0), (1, true), (2, '0'), (3, false);
SELECT * FROM test_bits_60000 where b = 1;

DROP TABLE IF EXISTS t1_60000;
DROP TABLE IF EXISTS t2_60000;
DROP TABLE IF EXISTS t3_60000;
DROP TABLE IF EXISTS t4_60000;
DROP TABLE IF EXISTS t5_60000;
DROP TABLE IF EXISTS t6_60000;
DROP TABLE IF EXISTS t7_60000;
DROP TABLE IF EXISTS t8_60000;
DROP TABLE IF EXISTS t9_60000;
CREATE TABLE t1_60000(a VARCHAR(10)) CHARACTER SET filename;
CREATE TABLE t2_60000(a VARCHAR(10)) COLLATE filename;
CREATE TABLE t3_60000(a VARCHAR(10) CHARACTER SET filename);
CREATE TABLE t4_60000(a VARCHAR(10) CHARACTER SET utf8) CHARACTER SET latin1;
CREATE TABLE t5_60000 (a char(16)) character set cp1250 collate cp1250_czech_cs;

CREATE TABLE t6_60000 (
    id INT NOT NULL,
    c INT NOT NULL,
    d INT NOT NULL,
    PRIMARY KEY (id)
);
CREATE TABLE t7_60000 (
    c INT NOT NULL,
    d INT NOT NULL,
    PRIMARY KEY (c, d)
);

create table t8_60000 (
	a int not null,
	b int not null,
	primary key (a,b),
	constraint fk1 foreign key (a) references t6_60000(c),
	constraint fk2 foreign key (a,b) references t6_60000 (c,d) on delete no action
	  on update no action,
	constraint fk3 foreign key (a,b) references t7_60000 (c,d) on update cascade,
	constraint fk4 foreign key (a,b) references t7_60000 (c,d) on delete set default,
	constraint fk5 foreign key (a,b) references t7_60000 (c,d) on update set null);

create table t9_60000 (
	a int not null,
	b int not null,
	primary key (a,b),
	foreign key (a,b) references t7_60000 (c,d) on delete no action
	  on update no action);


INSERT INTO t1_60000 VALUES ('');
SELECT a, length(a), a='', a=' ', a='  ' FROM t1_60000;
DROP TABLE IF EXISTS t1_60000;
DROP TABLE IF EXISTS t2_60000;
DROP TABLE IF EXISTS t3_60000;
DROP TABLE IF EXISTS t4_60000;
DROP TABLE IF EXISTS t5_60000;
DROP TABLE IF EXISTS t6_60000;
DROP TABLE IF EXISTS t7_60000;
DROP TABLE IF EXISTS t8_60000;
DROP TABLE IF EXISTS t9_60000;

DROP TABLE IF EXISTS users_60000;
DROP TABLE IF EXISTS users1_60000;
CREATE TABLE `users_60000` ( `uid` bigint(20) signed zerofill NOT NULL,`update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 CHECKSUM = 1;
CREATE TABLE `users1_60000` ( `uid` bigint(20) unsigned zerofill NOT NULL,`update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 CHECKSUM = 1 COMMENT = '';
DROP TABLE IF EXISTS users_60000;
DROP TABLE IF EXISTS users1_60000;

SHOW CREATE TABLE test_create_table_unique2;
SHOW CREATE TABLE mysql_create_ddl7;

DROP TABLE IF EXISTS mysql_create_ddl7;
DROP TABLE IF EXISTS test_create_table_unique1;
DROP TABLE IF EXISTS test_create_table_unique2;
DROP TABLE IF EXISTS record_60000;
DROP TABLE IF EXISTS test_bits_60000;
