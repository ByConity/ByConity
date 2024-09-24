-- use test
drop table if exists p3_source_prune_70000;
drop table if exists p4_source_prune_70000;

CREATE TABLE p3_source_prune_70000 (`a` String, `b` String) ENGINE = CnchMergeTree PARTITION BY a ORDER BY b;
CREATE TABLE p4_source_prune_70000 (`a` String, `b` String) ENGINE = CnchMergeTree PARTITION BY a ORDER BY b;

set enable_optimizer=1;
set enable_prune_source_plan_segment=1;

select 'union empty table';
select * from p3_source_prune_70000 union select * from p4_source_prune_70000;

select 'union empty & oneline';
insert into p4_source_prune_70000 values ('1', '1');
select a from p3_source_prune_70000 union select a from p4_source_prune_70000;
select 'empty';
select a from p3_source_prune_70000;
select 'oneline';
select a from p4_source_prune_70000;
select 'oneline & oneline';
insert into p3_source_prune_70000 values ('1', '1');
select a from p3_source_prune_70000 union select a from p4_source_prune_70000;

drop table if exists p3_source_prune_70000;
drop table if exists p4_source_prune_70000;

