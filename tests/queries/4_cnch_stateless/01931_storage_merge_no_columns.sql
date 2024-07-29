drop table if exists data;
create table data (key Int) engine=CnchMergeTree() order by key;
select 1 from merge(currentDatabase(0), '^data$') prewhere _table in (NULL);
drop table data;
