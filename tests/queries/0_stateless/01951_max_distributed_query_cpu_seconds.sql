
select count() from numbers(40);

drop table if exists dist_01951a;
drop table if exists data_01951a;
drop table if exists dist_01951b;
drop table if exists data_01951b;

create table data_01951a (date int, id int) engine=Memory();
insert into data_01951a select 1, number from numbers(5000000);
create table data_01951b (date int, id int) engine=Memory();
insert into data_01951b select 2, number from numbers(5000000);

create table dist_01951a as data_01951a engine=Distributed('test_shard_localhost', currentDatabase(), data_01951a);
create table dist_01951b as data_01951b engine=Distributed('test_shard_localhost', currentDatabase(), data_01951b);

select count(distinct *) from dist_01951a join dist_01951b using id settings max_distributed_query_cpu_seconds=1, enable_optimizer=1; -- { serverError 159 }

drop table dist_01951a;
drop table data_01951a;
drop table dist_01951b;
drop table data_01951b;