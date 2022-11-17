set enable_optimizer=1;
set dialect_type='ANSI';
set create_stats_time_output=0;
drop table if exists test_date_opt;
drop table if exists test_date_opt_local;
create table test_date_opt_local(
    id UInt64,
    date16 Date,
    date32 Date32,
    datetime32 DateTime32,
    datetime64 DateTime64(3),
    datetimewotz DateTimeWithoutTz,
    t Time
) Engine = MergeTree() order by id;

create table test_date_opt as test_date_opt_local Engine=Distributed(test_shard_localhost, currentDatabase(), test_date_opt_local, rand());

insert into test_date_opt values (0, '2022-09-27', '2022-09-27', '2022-09-27 00:00:00', '2022-09-27 00:00:00.010', '2022-09-27 00:00:00.010', '11:22:33');
insert into test_date_opt values (1, '2022-09-27', '2022-09-27', '2022-09-27 00:00:00', '2022-09-27 00:00:00.010', '2022-09-27 00:00:00.010', '11:22:33'),
insert into test_date_opt values (2, '2022-09-28', '2022-09-28', '2022-09-28 00:00:00', '2022-09-28 00:00:00.010', '2022-09-28 00:00:00.010', '05:20:30');

select * from test_date_opt order by id;
select '*** show stats all';
create stats test_date_opt;
select '*** test id';
explain select * from test_date_opt where id > 1;
select '*** test date16';
explain select * from test_date_opt where date32 == '2022-09-27';
select '*** test date32';
explain select * from test_date_opt where date32 == '2022-09-27';
select '*** test datetime32';
explain select * from test_date_opt where datetime32 == '2022-09-27 00:00:00';
select '*** test datetime64';
explain select * from test_date_opt where datetime64 == '2022-09-27 00:00:00.010';
select '*** test datetime without timezone';
explain select * from test_date_opt where datetimewotz == '2022-09-27 00:00:00.010';
select '*** test time';
explain select * from test_date_opt where t == '11:22:33';

drop stats test_date_opt;
drop table if exists test_date_opt;
drop table if exists test_date_opt_local;
