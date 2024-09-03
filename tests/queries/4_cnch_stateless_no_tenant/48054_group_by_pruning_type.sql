SET enable_group_by_keys_pruning = 1, cte_mode='INLINED', dialect_type='ANSI';
drop table if exists group_by_pruning;
drop table if exists group_by_pruning_local;

create table group_by_pruning (day Date, id UInt64) engine=CnchMergeTree() order by tuple();

insert into group_by_pruning select '2024-01-02', 1;

select b.day, count() from group_by_pruning a, group_by_pruning b where a.day = b.day and a.day = '2024-01-02' group by b.day, b.id;

drop table if exists group_by_pruning;
drop table if exists group_by_pruning_local;
