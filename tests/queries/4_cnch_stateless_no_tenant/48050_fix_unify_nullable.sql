set join_use_nulls=1;
create database if not exists test;
use test;
drop table if exists test.ad_positions;
drop table if exists test.ad_request_logs;
drop table if exists test.scenes;

CREATE TABLE test.ad_positions
(
    `id` UInt64,
    `scene_id` UInt8 COMMENT '广告场景ID'
)
ENGINE = CnchMergeTree
ORDER BY id;

CREATE TABLE test.ad_request_logs
(
    `id` UInt64,
    `ad_position_id` UInt16 COMMENT '广告位ID'
)
ENGINE = CnchMergeTree
ORDER BY id;

CREATE TABLE test.scenes
(
    `id` UInt64
)
ENGINE = CnchMergeTree
ORDER BY id;

insert into test.ad_positions values(1,2)(2,3)(3,4)(9,1);
insert into test.ad_request_logs values(1,2)(2,3)(3,4)(6,7)(11,12);
insert into test.scenes values(1)(2)(3)(7)(9)(10);

SELECT
    count(*)
FROM test.ad_request_logs
    LEFT JOIN test.ad_positions ON ad_request_logs.ad_position_id = ad_positions.id
    LEFT JOIN test.scenes ON ad_positions.scene_id = scenes.id;

drop TABLE if exists test.`51005_share_common_plan_node`;
CREATE TABLE 51005_share_common_plan_node
(
    `id` UInt32,
    `k1` UInt32,
    `k2` String
)
ENGINE = CnchMergeTree
ORDER BY id;
insert into 51005_share_common_plan_node values (1,1,'1');
set join_use_nulls=1;
set enable_optimizer=1;
select t1.id from 51005_share_common_plan_node t1 left join 51005_share_common_plan_node t2 on t1.id = t2.id and t1.k1 + t2.k1 > 0 where t1.k1 >= 1
