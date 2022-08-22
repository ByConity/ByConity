set collect_statistics_after_first_insertion=1;
drop table if exists t1;
create table t1 (
    value Int32
) ENGINE=CnchMergeTree() PARTITION BY value order by value;

select 'before insert';
show stats t1;

insert into t1 values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
select 'after first insert';
show stats t1;

select 'after second insert, no update';
insert into t1 values (11), (12), (13), (14), (15), (16), (17), (18), (19), (20);
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
show stats t1;

select 'disable auto stats';
set collect_statistics_after_first_insertion=0;
drop stats t1;
insert into t1 values (21), (22), (23), (24), (25), (26), (27), (28), (29), (30);
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
show stats t1;

select 're-enable auto stats';
set collect_statistics_after_first_insertion=1;
insert into t1 values (31), (32), (33), (34), (35), (36), (37), (38), (39), (40);
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
show stats t1;

drop table t1;
