drop table if EXISTS t_jsonb_3;
create table if not EXISTS t_jsonb_3 (id Int8, j JSONB) ENGINE=CnchMergeTree() order by id;
insert into t_jsonb_3 format JSONEachRow {"id":1, "j":"{\"a\":\"test\"}"};
select * from t_jsonb_3 order by id;
drop table if EXISTS t_jsonb_3;
