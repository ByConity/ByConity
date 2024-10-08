drop table if exists test_insert_optimizer;

create table test_insert_optimizer (id Int32) engine = CnchMergeTree order by id;

set enable_optimizer = 1;

insert into test_insert_optimizer select number from numbers(10);

insert into test_insert_optimizer select * from test_insert_optimizer;

insert into test_insert_optimizer 
    select id from test_insert_optimizer as a join
                   test_insert_optimizer as b on a.id = b.id;

select count() from test_insert_optimizer;

set max_insert_threads = 10;

insert into test_insert_optimizer select number from numbers(10);

insert into test_insert_optimizer select * from test_insert_optimizer;

insert into test_insert_optimizer 
    select id from test_insert_optimizer as a join
                   test_insert_optimizer as b on a.id = b.id;

select count() from test_insert_optimizer;

drop table if exists test_insert_optimizer;
