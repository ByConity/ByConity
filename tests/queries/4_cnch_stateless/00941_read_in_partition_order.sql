set enable_optimizer = 1;
set optimize_read_in_order = 1;
set optimize_read_in_partition_order = 1;
set force_read_in_partition_order = 1;

-- case: order by partition column
drop table if exists porder1;
create table porder1 (c1 Int64, c2 Int64) engine = CnchMergeTree partition by c1 order by (c1, c2);
insert into porder1 values (1, 1), (2, 2), (3, 3);
select 'order by c1';
select * from porder1 order by c1;
select 'order by c1 desc';
select * from porder1 order by c1 desc;
drop table porder1;

-- case: partition by func(sort_column) order by sort_column
drop table if exists porder2;
create table porder2 (ts DateTime) engine = CnchMergeTree partition by toDate(ts) order by ts;
insert into porder2 values ('2024-06-01 10:00:00'), ('2024-06-02 11:00:00'), ('2024-06-03 12:00:00');
select 'order by ts';
select * from porder2 order by ts;
select 'order by ts desc';
select * from porder2 order by ts desc;
drop table porder2;

-- case: order by (.., pc, ..) partitoin by pc
drop table if exists porder3;
create table porder3 (c1 Int64, c2 String, d Date, c3 Int64) engine = CnchMergeTree partition by d order by (c1, c2, d, c3);
insert into porder3 values (1, 'a', '2024-06-01', 10), (2, 'b', '2024-06-01', 20), (3, 'c', '2024-06-02', 30), (1, 'a', '2024-06-03', 40), (1, 'a', '2024-06-03', 50);
select 'order by d, c3';
select * from porder3 where c1=1 and c2='a' and d < '2024-06-10' and c3 < 100 order by d, c3;
select 'order by d desc, c3';
select * from porder3 where c1=1 and c2='a' and d < '2024-06-10' and c3 < 100 order by d desc, c3;
drop table porder3;

-- case: partition has more than 1 part
drop table if exists porder4;
create table porder4 (ts DateTime, c1 Int64, c2 String) engine = CnchMergeTree partition by toYYYYMMDD(ts) order by (c1, c2, ts);
system stop merges porder4;
insert into porder4 values ('2024-06-01 11:00:00', 5, 'a'), ('2024-06-01 10:00:00', 6, 'a');
insert into porder4 values ('2024-06-01 10:00:00', 5, 'a'), ('2024-06-01 11:00:00', 6, 'a');
insert into porder4 values ('2024-06-02 11:00:00', 5, 'a'), ('2024-06-02 10:00:00', 6, 'a');
insert into porder4 values ('2024-06-02 10:00:00', 5, 'a'), ('2024-06-02 11:00:00', 6, 'a');
select 'first 5', * from porder4 where c1=5 and c2='a' order by ts limit 1;
select 'first 6', * from porder4 where c1=6 and c2='a' order by ts limit 1;
select 'last 5',  * from porder4 where c1=5 and c2='a' order by ts desc limit 1;
select 'last 6',  * from porder4 where c1=6 and c2='a' order by ts desc limit 1;
drop table porder4;

-- case: partition by tuple with monotonicity hint
drop table if exists porder5;
create table porder5 (ts DateTime, c1 Int64) engine = CnchMergeTree partition by (toDate(ts), toHour(ts)) order by (c1, ts) settings partition_by_monotonicity_hint=1;
insert into porder5 values ('2024-07-01 01:00:00', 1);
insert into porder5 values ('2024-07-01 02:00:00', 1);
insert into porder5 values ('2024-07-01 03:00:00', 1);
insert into porder5 values ('2024-07-02 01:00:00', 1);
select 'porder5 first ts', * from porder5 where c1 = 1 order by ts limit 1;
select 'porder5 last  ts', * from porder5 where c1 = 1 order by ts desc limit 1;
drop table porder5;

-- negative case: partition by non-atomic function
drop table if exists norder1;
create table norder1 (c1 Int64) engine = CnchMergeTree partition by c1 % 4 order by c1;
insert into norder1 select number from numbers(10);
select * from norder1 order by c1 limit 1; -- { serverError 277 }
select 'norder1';
select * from norder1 order by c1 limit 1 settings force_read_in_partition_order=0;
drop table norder1;

-- negative case: sort by non-partition column
drop table if exists norder2;
create table norder2 (c1 Int64, c2 Int64) engine = CnchMergeTree partition by c1 order by c2;
insert into norder2 values (1, 2), (2, 1);
select * from norder2 order by c2; -- { serverError 277 }
select 'norder2';
select * from norder2 order by c2 settings force_read_in_partition_order=0;
drop table norder2;

-- negative case: partition by multi-func
drop table if exists norder3;
create table norder3 (ts DateTime) engine = CnchMergeTree partition by (toYYYYMMDD(ts) % 2) order by ts;
insert into norder3 values ('2024-06-01 00:00:00'), ('2024-06-02 00:00:00');
select * from norder3 order by ts; -- { serverError 277 }
drop table norder3;

-- negative case: no equal predicate on prefix sort column
drop table if exists norder4;
create table norder4 (c1 Int64, c2 Int64) engine = CnchMergeTree order by (c1, c2) partition by c2;
insert into norder4 select number, 1 from numbers(5);
select * from norder4 order by c2; -- { serverError 277 }
select * from norder4 where c1 < 2 order by c2; -- { serverError 277 }
drop table norder4;

-- negative case: partition by tuple wo/ monotonicity hint
drop table if exists norder5;
create table norder5 (ts DateTime, c1 Int64) engine = CnchMergeTree partition by (toHour(ts), toDate(ts)) order by (c1, ts);
insert into norder5 select toDateTime('2024-06-01 00:00:00') + interval number hour, 1 from numbers(3);
select * from norder5 where c1 = 1 order by ts limit 1; -- { serverError 277 }
drop table norder5;
