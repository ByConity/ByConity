USE test;
drop table if exists test.lc;
create table test.lc (b LowCardinality(String)) engine=CnchMergeTree order by b;
insert into test.lc select '0123456789' from numbers(10000000);
select count(), b from test.lc group by b;
drop table if exists test.lc;
