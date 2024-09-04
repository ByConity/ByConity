set dialect_type = 'ANSI';
set enable_optimizer = 1;


create table t48019 (
  somedate Date,
  id UInt64,
  data String
) engine = CnchMergeTree() order by id;

insert into t48019 values ('2022-01-01', 1, '1');
insert into t48019 values ('2023-01-01', 2, '2');
insert into t48019 values ('2024-01-01', 3, '3');

select sum(cityHash64(*)) from t48019;