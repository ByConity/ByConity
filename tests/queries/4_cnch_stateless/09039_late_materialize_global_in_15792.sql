drop table if exists xp_d;

create table xp_d(A Date, B Int64, S String) Engine=CnchMergeTree partition by toYYYYMM(A) order by B SETTINGS enable_late_materialize = 1;
insert into xp_d select '2020-01-01', number , '' from numbers(100000);

select count() from xp_d where toYYYYMM(A) global in (select toYYYYMM(min(A)) from xp_d) AND B > -1;

drop table if exists xp_d;
