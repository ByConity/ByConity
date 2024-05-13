drop DATABASE if exists test_48044;
CREATE DATABASE test_48044;

use test_48044;
drop table if exists test1;

create table test1  (a String, b String NOT NULL, c Int64) engine = CnchMergeTree() order by a;

insert into test1 values ('a', '1', 1);
insert into test1 values ('b', '1', 1);
insert into test1 values ('c', '1', 2);
insert into test1 values ('d', '4', 2);
insert into test1 values ('d', '5', 3);

select count(*), sum(c), count(distinct a), count(distinct b) from test1;



