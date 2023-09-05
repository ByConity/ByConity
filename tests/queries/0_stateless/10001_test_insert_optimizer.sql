drop table if exists test.test_insert_optimizer;
drop table if exists test.test_insert_optimizer_local;

create table test.test_insert_optimizer_local (id Int32) engine = MergeTree order by id;
create table test.test_insert_optimizer as test.test_insert_optimizer_local engine = Distributed(test_shard_localhost, test, test_insert_optimizer_local, rand());

set enable_optimizer = 1;
set enable_optimizer_white_list = 0;

insert into test.test_insert_optimizer select number from numbers(10);

insert into test.test_insert_optimizer select * from test.test_insert_optimizer;

insert into test.test_insert_optimizer 
    select id from test.test_insert_optimizer as a join
                   test.test_insert_optimizer as b on a.id = b.id;

select count() from test.test_insert_optimizer;

set max_insert_threads = 10;

insert into test.test_insert_optimizer select number from numbers(10);

insert into test.test_insert_optimizer select * from test.test_insert_optimizer;

insert into test.test_insert_optimizer 
    select id from test.test_insert_optimizer as a join
                   test.test_insert_optimizer as b on a.id = b.id;

select count() from test.test_insert_optimizer;

drop table if exists test.test_insert_optimizer;
drop table if exists test.test_insert_optimizer_local;