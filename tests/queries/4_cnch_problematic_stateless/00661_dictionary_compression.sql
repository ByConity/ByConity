set enable_dictionary_compression=1;
drop table if exists test.cp;

create table test.cp (id UInt16 COMPRESSION, date Date) engine=CnchMergeTree() PARTITION BY toYYYYMM(date) ORDER BY (id, date) SETTINGS index_granularity=8192;

insert into table test.cp values (1, '2018-01-01'),(2, '2018-01-02'),(3, '2018-01-03');

select * from test.cp order by id;

select id from test.cp where id = 1 order by id;
select id, count() from test.cp group by id order by id;
select id from test.cp where id != 2 order by id;
select id from test.cp where id in (1, 2) order by id;

drop table test.cp;

create table test.cp (id UInt16, date Date, info String COMPRESSION) engine=CnchMergeTree() PARTITION BY toYYYYMM(date) ORDER BY (id, date, info) SETTINGS index_granularity=8192;

insert into table test.cp values (1, '2018-01-01', 'info1'),(2, '2018-01-03', 'info2'),(3, '2018-01-03', 'info3');

select * from test.cp order by id;
select info, count() from test.cp group by info order by info;
select info from test.cp where info != 'info1' order by info;
select info from test.cp where info = 'info1' order by info;
select info from test.cp where info in ('info1', 'info2') order by info;

select * from test.cp order by id;

drop table test.cp;

