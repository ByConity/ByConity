drop table if exists prewhere_alias;
create table prewhere_alias (a Int32, b Int32, c alias a + b) engine = CnchMergeTree order by b settings enable_late_materialize = 1;
insert into prewhere_alias values(1, 1);
select a, c + toInt32(1), (c + toInt32(1)) * 2 from prewhere_alias where (c + toInt32(1)) * 2 = 6;
drop table prewhere_alias;
