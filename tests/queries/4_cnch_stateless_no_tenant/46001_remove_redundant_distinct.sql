DROP database if exists test_distinct;

create database test_distinct;

use test_distinct;

DROP TABLE IF EXISTS test_distinct.unique_1;
DROP TABLE IF EXISTS test_distinct.unique_2;
DROP TABLE IF EXISTS test_distinct.unique_3;

CREATE TABLE test_distinct.unique_1 (event_time DateTime, id UInt64, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree(m1) PARTITION BY toDate(event_time) ORDER BY id UNIQUE KEY id;
insert into test_distinct.unique_1 select toDate('2021-01-01') + number, number, number % 5, number % 10 from numbers(20);

CREATE TABLE test_distinct.unique_2 (event_time DateTime, id UInt64, id2 UInt32, m2 UInt64) ENGINE = CnchMergeTree(id2) PARTITION BY toDate(event_time) ORDER BY id UNIQUE KEY (id,id2);
insert into test_distinct.unique_2 select toDate('2021-01-01') + number, number, number % 20, number % 10 from numbers(20,20);

CREATE TABLE test_distinct.unique_3 (event_time DateTime, id3 UInt64, m1 UInt32, m2 UInt64, m3 UInt32) ENGINE = CnchMergeTree(m1) PARTITION BY toDate(event_time) ORDER BY id3 UNIQUE KEY id3;
insert into test_distinct.unique_3 select toDate('2021-01-01') + number, number, number % 5, number % 10, number % 2 from numbers(40,10);

CREATE TABLE test_distinct.unique_4 (event_time DateTime, id4 UInt64, m1 UInt32, m2 UInt64, k3 String) ENGINE = CnchMergeTree(m1) PARTITION BY toDate(event_time) ORDER BY id4 UNIQUE KEY (id4,m2,sipHash64(k3));
insert into test_distinct.unique_4 select toDate('2021-01-01') + number, number, number % 5, number % 10, number % 2 from numbers(50,10);

set enable_optimizer=1;
set enable_distinct_remove=1;
set enum_replicate_no_stats=0;
set send_logs_level='warning';

select '---------test union';
explain select distinct id
        from
            (
                select distinct id from test_distinct.unique_1
                union all
                select distinct id from test_distinct.unique_2
            )id;

select '---------test union2';
explain select distinct id
from
    (
        select distinct id from test_distinct.unique_1
        union all
        select distinct id3 from test_distinct.unique_3
    )id ;

select '---------test inner join1';
explain select distinct unique_1.id
        from test_distinct.unique_1
                 inner join test_distinct.unique_2
                            On unique_1.id = unique_2.id;


select '---------test inner join2';
explain select distinct unique_1.id
        from test_distinct.unique_1
                 inner join test_distinct.unique_3
                            On unique_1.id = unique_3.id3;


select '---------test inner join3';
explain select distinct unique_1.id  from test_distinct.unique_1 inner join test_distinct.unique_2 On unique_1.id = unique_2.id and unique_1.m1 = unique_2.id2;


select '---------test left join';
explain select distinct id
        from test_distinct.unique_1
                 left join test_distinct.unique_3
                           On unique_1.id = unique_3.id3;


select '---------test right join';
explain select distinct id3
        from test_distinct.unique_1
                 right join test_distinct.unique_3
                            On unique_1.id = unique_3.id3;


select '---------test full join';
explain select distinct id
        from test_distinct.unique_1
                 full outer join test_distinct.unique_3
                                 On unique_1.id = unique_3.id3;


select '---------test cte';
set dialect_type='ANSI';
set cte_mode='AUTO'; -- ToDo(WangTao): remove this
explain with dw as (select distinct id as avg_id from test_distinct.unique_1)
select distinct id3 + 10 from  test_distinct.unique_3
where unique_3.id3 >(select sum(avg_id)/25 from dw) and unique_3.id3 > (select sum(avg_id)/20 from dw);
select '---------test cte2';
explain with dw as (select id  from test_distinct.unique_1) select  id from (select distinct id from dw union all select distinct id from dw)id;


select '---------test groupby';
explain select distinct m1,m2 from (select m1, m2, max(id) from unique_1 group by m1,m2) where m1 >1;

select '---------test sipHash(64)';
explain select distinct id4,m2,k3 from unique_4;



drop table if exists test_distinct.unique_1;
drop table if exists test_distinct.unique_2;
drop table if exists test_distinct.unique_3;
drop table if exists test_distinct.unique_4;

DROP database if exists test_distinct;