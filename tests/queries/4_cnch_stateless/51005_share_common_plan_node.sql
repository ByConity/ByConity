CREATE TABLE 51005_share_common_plan_node
(
    `id` UInt32,
    `k1` UInt32,
    `k2` String
)
ENGINE = CnchMergeTree
ORDER BY id;

insert into 51005_share_common_plan_node values (1,1,'1');

set enable_optimizer=1;
set enable_share_common_plan_node=1;
set dialect_type='ANSI';

explain select id from 
(
    select t1.id from 51005_share_common_plan_node t1 left join 51005_share_common_plan_node t2 on t1.id = t2.id and t1.k1 > 0 where t1.k1 >= 1
) union all (
    select t1.id from 51005_share_common_plan_node t1 left join 51005_share_common_plan_node t2 on t1.id = t2.id and t1.k1 > 0 where t1.k1 >= 2
);

select id from 
(
    select t1.id from 51005_share_common_plan_node t1 left join 51005_share_common_plan_node t2 on t1.id = t2.id and t1.k1 > 0 where t1.k1 >= 1
) union all (
    select t1.id from 51005_share_common_plan_node t1 left join 51005_share_common_plan_node t2 on t1.id = t2.id and t1.k1 > 0 where t1.k1 >= 2
);