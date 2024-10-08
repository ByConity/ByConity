set dialect_type = 'ANSI';
set enable_optimizer_for_create_select = 1;

drop table if exists t40078;
create table t40078 ENGINE = CnchMergeTree ORDER BY tuple() AS with tmp as (select 1 as a) select * from tmp;
drop table if exists t40078;
