drop table if exists test1;

create table test1(i int, j int) engine = MergeTree partition by i order by tuple() settings index_granularity = 1;

insert into test1 select number, number + 100 from numbers(10);
select partitionStatus('default', 'test1', '2');

drop table test1;
