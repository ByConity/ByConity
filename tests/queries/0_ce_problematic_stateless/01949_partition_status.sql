use test;
drop table if exists number_table;

create table number_table(i int, j int) engine = MergeTree partition by i order by tuple() settings index_granularity = 1;

insert into number_table select number, number + 100 from numbers(10);
select partitionStatus('test', 'number_table', '2');

drop table number_table;
