USE test;
drop table if exists test.replace_dst;
drop table if exists test.replace_src;

create table if not exists test.replace_dst (date Date, id UInt64) Engine = CnchMergeTree partition by date order by id;

insert into test.replace_dst values ('2019-01-01', 1);
insert into test.replace_dst values ('2019-01-02', 1);
insert into test.replace_dst values ('2019-01-03', 1);

create table if not exists test.replace_src as test.replace_dst;

insert into test.replace_src select * from test.replace_dst;

select count() from system.cnch_parts where database = 'test' and table = 'replace_dst' and active;

alter table test.replace_dst replace partition id '20190101' from test.replace_src;
select count() from system.cnch_parts where database = 'test' and table = 'replace_dst' and active;
select count() from system.cnch_parts where database = 'test' and table = 'replace_src' and active;

alter table test.replace_dst replace partition where date = '2019-01-02' from test.replace_src;
select count() from system.cnch_parts where database = 'test' and table = 'replace_dst' and active;
select count() from system.cnch_parts where database = 'test' and table = 'replace_src' and active;

alter table test.replace_dst replace partition where date > '2019-01-02' from test.replace_src;
select count() from system.cnch_parts where database = 'test' and table = 'replace_dst' and active;
select count() from system.cnch_parts where database = 'test' and table = 'replace_src' and active;

drop table if exists test.replace_dst;
drop table if exists test.replace_src;