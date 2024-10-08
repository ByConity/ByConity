drop table if EXISTS t_jsonb_2;
create table if not EXISTS t_jsonb_2 (id Int8, j JSONB) ENGINE=CnchMergeTree() order by id;
insert into t_jsonb_2 values (1, 'true');
insert into t_jsonb_2 values (2, '{]');
insert into t_jsonb_2 values (3, '100');
insert into t_jsonb_2 values (4, '')
select * from t_jsonb_2 order by id;
drop table if EXISTS t_jsonb_2;
