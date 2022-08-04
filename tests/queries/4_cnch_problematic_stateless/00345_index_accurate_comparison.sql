USE test;
DROP TABLE IF EXISTS index;
CREATE TABLE index
(
    key Int32,
    name String,
    merge_date Date
) ENGINE = CnchMergeTree PARTITION BY merge_date ORDER BY key SETTINGS index_granularity = 8192;
insert into index values (1,'1','2016-07-07');
insert into index values (-1,'-1','2016-07-07');
select * from index where key = 1;
select * from index where key = -1;
OPTIMIZE TABLE test.index;
select * from index where key = 1;
select * from index where key = -1;
select * from index where key < -0.5;
DROP TABLE index;
