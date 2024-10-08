drop table if EXISTS t_jsonb_1;
create table if not EXISTS t_jsonb_1 (id Int8, j JSONB) ENGINE=CnchMergeTree() order by id;
insert into t_jsonb_1 values (1, '{"a":1, "b":2}');
insert into t_jsonb_1 values (2, '{"c":3, "d":4}');
select * from t_jsonb_1 order by id;
drop table if EXISTS t_jsonb_1;
