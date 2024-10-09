set dialect_type='MYSQL';

DROP TABLE IF EXISTS mysql_create_select_ddl1;
DROP TABLE IF EXISTS mysql_create_select_ddl2;
DROP TABLE IF EXISTS mysql_create_select_ddl3;
DROP TABLE IF EXISTS mysql_create_select_ddl4;
DROP TABLE IF EXISTS mysql_create_select_ddl5;
DROP TABLE IF EXISTS mysql_create_select_ddl6;
CREATE TABLE mysql_create_select_ddl1
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

CREATE TABLE mysql_create_select_ddl2 ENGINE=CnchMergeTree() ORDER BY id AS SELECT * FROM mysql_create_select_ddl1;
CREATE TABLE mysql_create_select_ddl3 ORDER BY id AS SELECT * FROM mysql_create_select_ddl1;
CREATE TABLE mysql_create_select_ddl4 AS SELECT * FROM mysql_create_select_ddl1;
CREATE TABLE mysql_create_select_ddl5 PRIMARY KEY(id) AS SELECT id FROM mysql_create_select_ddl1;
CREATE TABLE mysql_create_select_ddl6 (id Int32, PRIMARY KEY(id)) AS SELECT id FROM mysql_create_select_ddl1;


drop table if EXISTS mysql_create_select_t1;
drop table if EXISTS mysql_create_select_t2;
drop table if EXISTS mysql_create_select_t3;
drop table if EXISTS mysql_create_select_t4;
create table mysql_create_select_t1 (a int, b int, c int);
create table mysql_create_select_t2 (d int);
create table mysql_create_select_t3 (a1 int, b1 int, c1 int);
insert into mysql_create_select_t1 values(1,2,3);
insert into mysql_create_select_t1 values(11,22,33);
insert into mysql_create_select_t2 values(99);
create table mysql_create_select_t4 select mysql_create_select_t1.* from mysql_create_select_t1;
select * from mysql_create_select_t4 ORDER BY a;
drop table mysql_create_select_t4;
create table mysql_create_select_t4 select mysql_create_select_t2.*, 1, 2 from mysql_create_select_t2;
select * from mysql_create_select_t4 ORDER BY d;
drop table mysql_create_select_t4;
drop table mysql_create_select_t1;
drop table mysql_create_select_t2;
drop table mysql_create_select_t3;

drop table if EXISTS mysql_create_select_t5;
CREATE TABLE mysql_create_select_t5(a String, b String) AS SELECT CONCAT(CAST(REPEAT('9', 1000) AS String)), CONCAT(CAST(REPEAT('9', 1000) AS String));
drop table if EXISTS mysql_create_select_t5;

drop table if EXISTS mysql_create_select_t6;
create table mysql_create_select_t6 select last_day('2000-02-05') as a,
                from_days(to_days('960101')) as b;
select * from mysql_create_select_t6;
drop table if EXISTS mysql_create_select_t6;

drop table if EXISTS mysql_create_select_t7;
create table mysql_create_select_t7 select now() - now(), curtime() - curtime(),
                       sec_to_time(1) + 0, from_unixtime(1) + 0;
show create table mysql_create_select_t7;
drop table if EXISTS mysql_create_select_t7;

drop table if EXISTS mysql_create_select_t8;
drop table if EXISTS mysql_create_select_t9;
create table mysql_create_select_t8
(
  a char(8) not null,
  b char(20) not null,
  c int not null,
  key (a)
)engine=heap;

insert into mysql_create_select_t8 values ('aaaa', 'prefill-hash=5',0);
insert into mysql_create_select_t8 values ('aaab', 'prefill-hash=0',0);
create table mysql_create_select_t9 as select * from mysql_create_select_t8;
select * from mysql_create_select_t8 ORDER BY a;
drop table if EXISTS mysql_create_select_t8;
drop table if EXISTS mysql_create_select_t9;

drop table if EXISTS mysql_create_select_t10;
drop table if EXISTS mysql_create_select_t11;
CREATE TABLE mysql_create_select_t10 (a DECIMAL (1, 0), b DECIMAL (1, 0));
INSERT INTO mysql_create_select_t10 (a, b) VALUES (0, 0);

CREATE TABLE mysql_create_select_t11 SELECT IFNULL(a, b) FROM mysql_create_select_t10;
DESCRIBE mysql_create_select_t11;
DROP TABLE mysql_create_select_t11;

CREATE TABLE mysql_create_select_t11 SELECT IFNULL(a, NULL) FROM mysql_create_select_t10;
DESCRIBE mysql_create_select_t11;
DROP TABLE mysql_create_select_t11;

CREATE TABLE mysql_create_select_t11 SELECT IFNULL(NULL, b) FROM mysql_create_select_t10;
DESCRIBE mysql_create_select_t11;

drop table if EXISTS mysql_create_select_t10;
drop table if EXISTS mysql_create_select_t11;

show create table mysql_create_select_ddl2;
show create table mysql_create_select_ddl3;
show create table mysql_create_select_ddl4;
show create table mysql_create_select_ddl5;
show create table mysql_create_select_ddl6;
DROP TABLE IF EXISTS mysql_create_select_ddl1;
DROP TABLE IF EXISTS mysql_create_select_ddl2;
DROP TABLE IF EXISTS mysql_create_select_ddl3;
DROP TABLE IF EXISTS mysql_create_select_ddl4;
DROP TABLE IF EXISTS mysql_create_select_ddl5;
DROP TABLE IF EXISTS mysql_create_select_ddl6;

DROP TABLE IF EXISTS 60001_customer;
DROP TABLE IF EXISTS 60001_customer_add_fk;
CREATE TABLE 60001_customer (
customer_id bigint NOT NULL COMMENT '顾客ID',
customer_name varchar NOT NULL COMMENT '顾客姓名',
phone_num bigint NOT NULL COMMENT '电话',
city_name varchar NOT NULL COMMENT '所属城市',
sex int NOT NULL COMMENT '性别',
id_number varchar NOT NULL COMMENT '身份证号码',
home_address varchar NOT NULL COMMENT '家庭住址',
office_address varchar NOT NULL COMMENT '办公地址',
age int NOT NULL COMMENT '年龄',
login_time timestamp NOT NULL COMMENT '登录时间',
PRIMARY KEY (login_time,customer_id,phone_num)
)
DISTRIBUTED BY HASH(customer_id)
PARTITION BY VALUE(DATE_FORMAT(login_time, '%Y%m%d'))
TTL to_date(login_time) + toIntervalDay(30)
COMMENT '客户信息表';

CREATE TABLE 60001_customer_add_fk
(
    FOREIGN KEY (age) REFERENCES 60001_customer (customer_id)
)
AS
SELECT * FROM test_adb_qinlei.customer; -- { serverError 90 }

DROP TABLE IF EXISTS 60001_customer;
DROP TABLE IF EXISTS 60001_customer_add_fk;