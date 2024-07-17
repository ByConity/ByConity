set dialect_type='MYSQL';
drop table if exists t1;
CREATE TABLE t1
(
    `id` int,
    `date` Date,
    `val1` Date
)
ENGINE = CnchMergeTree
PARTITION BY date
ORDER BY id
TTL toDateTime(val1) + 30;   -- { serverError 538 }

CREATE TABLE t1
(
    `id` int,
    `date` Date,
    `val1` Date
)
ENGINE = CnchMergeTree
PARTITION BY date
ORDER BY id
TTL toDate(val1) + 30;   -- { serverError 538 }

CREATE TABLE t1
(
    `id` int,
    `date` Date,
    `val1` Date
)
ENGINE = CnchMergeTree
PARTITION BY date
ORDER BY id
TTL toDate(val1) + INTERVAL 30 DAY;

show create table t1;
alter table t1 rename column val1 to new_val1;
show create table t1;
drop table t1;
