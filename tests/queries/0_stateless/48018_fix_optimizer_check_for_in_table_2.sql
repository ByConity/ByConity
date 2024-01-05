create database if not exists test_optimizer_in_table;
use test_optimizer_in_table;
create table if not exists test_local (c1 String not null) engine = MergeTree order by c1;
create table if not exists test_all as test_local engine=Distributed('test_shard_localhost', currentDatabase(), 'test_local');

insert into test_local values ('0');

set enable_optimizer=1;
set enum_replicate_no_stats=0;

explain select * from test_all where c1 in test_all and c1 in test_optimizer_in_table.test_all;
select * from test_all where c1 in test_all and c1 in test_optimizer_in_table.test_all;

drop table test_local;
drop table test_all;