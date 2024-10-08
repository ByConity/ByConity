drop table if exists t;
create table t(iclass_lvl_1 String, iclass_lvl_2 String) engine = CnchMergeTree() order by tuple();

insert into t values ('a', '1') ('b', '2');

set enable_optimizer=1;
set optimize_move_to_prewhere=1;

select iclass_lvl_1, iclass_lvl_2 from t where iclass_lvl_1 = 'a';
explain select iclass_lvl_1, iclass_lvl_2 from t where iclass_lvl_1 = 'a';
drop table if exists t;
