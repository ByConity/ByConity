set allow_suspicious_low_cardinality_types = 1;
SET mutations_sync = 1;
drop table if exists tab_00718;

-- set max_addition_bg_task_num = 0 to stop merge select.
create table tab_00718 (a String, b LowCardinality(UInt32)) engine = CnchMergeTree order by a SETTINGS max_addition_bg_task_num = 0;
SYSTEM START MERGES tab_00718;

insert into tab_00718 values ('a', 1);
select *, toTypeName(b) from tab_00718;

alter table tab_00718 modify column b UInt32;
select *, toTypeName(b) from tab_00718;

alter table tab_00718 modify column b LowCardinality(UInt32);
select *, toTypeName(b) from tab_00718;

alter table tab_00718 modify column b StringWithDictionary;
select *, toTypeName(b) from tab_00718;

alter table tab_00718 modify column b LowCardinality(UInt32);
select *, toTypeName(b) from tab_00718;

alter table tab_00718 modify column b String;
select *, toTypeName(b) from tab_00718;

alter table tab_00718 modify column b LowCardinality(UInt32);
select *, toTypeName(b) from tab_00718;

drop table if exists tab_00718;
