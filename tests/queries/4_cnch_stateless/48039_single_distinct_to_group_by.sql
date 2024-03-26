drop DATABASE if exists test_48039;
CREATE DATABASE test_48039;

use test_48039;
drop table if exists test1;
drop table if exists test2;

create table test1  (a String, b String NOT NULL, c String) engine = CnchMergeTree() order by b ;
create table test2  (a String, b String NOT NULL, c String) engine = CnchMergeTree() order by b;

insert into test1 values ('a', '1', 'a');
insert into test1 values ('a', '2', 'a');
insert into test1 values ('a', '3', 'a');
insert into test1 values ('a', '4', 'a');
insert into test1 values ('a', '5', 'a');

insert into test2 values ('a', 1, 'a');
insert into test2 values ('a', 2, 'a');
insert into test2 values ('a', 3, 'a');
insert into test2 values ('a', 4, 'a');
insert into test2 values ('a', 5, 'a');

set enable_optimizer=1;

select count(distinct a) from test1 group by b ;

# Aggregate function count requires zero or one argument
select count(distinct a, b) from test1 group by c;

select count(distinct a), max(distinct a) from test1 group by b,c ;

select count(distinct a), count(distinct b) from test1 group by c;