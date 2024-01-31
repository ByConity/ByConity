drop table if exists test_in;
create table test_in (a LowCardinality(String)) Engine = CnchMergeTree order by a SETTINGS enable_late_materialize = 1;

insert into test_in values ('a');
select * from test_in where a in ('a');

drop table if exists test_in;
