drop table if exists t_00712_2;
create table t_00712_2 (date Date, counter UInt64, sampler UInt64, alias_col alias date + 1) engine = CnchMergeTree PARTITION BY toMonth(date) SAMPLE BY intHash32(sampler) ORDER BY (counter, date, intHash32(sampler)) SETTINGS enable_late_materialize = 1;
insert into t_00712_2 values ('2018-01-01', 1, 1);
select alias_col from t_00712_2 sample 1 / 2 where date = '2018-01-01' and counter = 1 and sampler = 1;
drop table if exists t_00712_2;

