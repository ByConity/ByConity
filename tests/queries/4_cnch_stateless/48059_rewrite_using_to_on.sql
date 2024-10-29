set enable_optimizer = 1;

drop table if exists t1;
drop table if exists t2;

create table t1 (a UInt64) Engine = CnchMergeTree() ORDER BY a;
create table t2 (a UInt64) Engine = CnchMergeTree() ORDER BY a;
insert into t1 select number from system.numbers limit 100;
insert into t2 select number from system.numbers limit 100000;

create stats t1 format Null;
create stats t2 format Null;

set enable_join_using_to_join_on = 0;
set enum_replicate=0;
explain stats=0, verbose=0
select a from 
    t1
left join 
    t2
using (a);

explain stats=0, verbose=0
select a from 
    t1
inner join 
    t2
using (a);

set enable_join_using_to_join_on = 1;
explain stats=0, verbose=0
select a from 
    t1
left join 
    t2
using (a);

explain stats=0, verbose=0
select a from 
    t1
inner join 
    t2
using (a);