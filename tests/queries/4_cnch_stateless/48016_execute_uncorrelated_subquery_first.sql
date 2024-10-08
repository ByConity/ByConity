DROP TABLE IF EXISTS uncorrelated;
DROP TABLE IF EXISTS uncorrelated2;
set enable_optimizer=1;
CREATE TABLE uncorrelated
(
    a Int32,
    b UInt8
) ENGINE = CnchMergeTree()
ORDER BY a;

CREATE TABLE uncorrelated2
(
    a Int32,
    b UInt8
) ENGINE = CnchMergeTree()
ORDER BY a;

insert into uncorrelated values(1,2)(2,3)(3,4)(4,5)(5,6)(2,1);
insert into uncorrelated2 values(3,4)(4,5)(5,6)(2,1);

set enable_execute_uncorrelated_subquery=0;
select * from uncorrelated where uncorrelated.a < (select count() from uncorrelated2) order by a,b;
select * from uncorrelated where uncorrelated.a < (select 10) order by a,b;
select * from uncorrelated where uncorrelated.a < (select 10 + 1) order by a,b;
select * from uncorrelated where uncorrelated.a in (select toInt32(1+2)) order by a,b;
select a from uncorrelated WHERE EXISTS(select a from uncorrelated2 where a < 10) order by a;
select a from uncorrelated WHERE NOT EXISTS(select a from uncorrelated2 where a > 10) order by a;

set enable_execute_uncorrelated_subquery=1;
select * from uncorrelated where uncorrelated.a < (select count() from uncorrelated2) order by a,b;
select * from uncorrelated where uncorrelated.a < (select 10) order by a,b;
select * from uncorrelated where uncorrelated.a < (select 10 + 1) order by a,b;
select * from uncorrelated where uncorrelated.a in (select toInt32(1+2)) order by a,b;
select a from uncorrelated WHERE EXISTS(select a from uncorrelated2 where a < 10) order by a;
select a from uncorrelated WHERE NOT EXISTS(select a from uncorrelated2 where a > 10) order by a;

explain select * from uncorrelated where uncorrelated.a < (select max(a) from uncorrelated2) order by a,b;
explain select * from uncorrelated where uncorrelated.a < (select 10) order by a,b;
explain select * from uncorrelated where uncorrelated.a < (select 10 + 1) order by a,b;
explain select a from uncorrelated WHERE EXISTS(select a from uncorrelated2 where a < 10) order by a;
explain select a from uncorrelated WHERE NOT EXISTS(select a from uncorrelated2 where a > 10) order by a;

DROP TABLE IF EXISTS uncorrelated;
DROP TABLE IF EXISTS uncorrelated2;

DROP TABLE IF EXISTS dm_mint_crm_creator_stats_df;
CREATE TABLE dm_mint_crm_creator_stats_df
(
    `date` Date,
    `followed_counter_td` Nullable(Int64),
    `ops_owner_id` Int64,
    `user_id` Int64
)
    ENGINE = CnchMergeTree
PARTITION BY date
ORDER BY (ops_owner_id, intHash64(user_id))
SAMPLE BY intHash64(user_id);
insert into dm_mint_crm_creator_stats_df values('2023-07-13',1,2,3)('2023-07-13',2,3,4)('2023-07-12',3,4,5)('2023-07-11',4,5,6)

SELECT `user_id`
FROM `dm_mint_crm_creator_stats_df` AS dm_mint_crm_creator_stats_df
WHERE `date` =
      (
          SELECT MAX(`date`)
          FROM `dm_mint_crm_creator_stats_df` AS dm_mint_crm_creator_stats_df
      )
ORDER BY `followed_counter_td` DESC
    LIMIT 0, 1000
SETTINGS enable_optimize_predicate_expression = 0, distributed_product_mode = 'global', enable_optimize_predicate_expression = 1, distributed_product_mode = 'local';

set enable_optimizer=1;
explain SELECT `user_id`
FROM `dm_mint_crm_creator_stats_df` AS dm_mint_crm_creator_stats_df
WHERE `date` =
      (
          SELECT MAX(`date`)
          FROM `dm_mint_crm_creator_stats_df` AS dm_mint_crm_creator_stats_df
      )
ORDER BY `followed_counter_td` DESC
    LIMIT 0, 1000
SETTINGS enable_execute_uncorrelated_subquery=1, enable_optimize_predicate_expression = 0, distributed_product_mode = 'global', enable_optimize_predicate_expression = 1, distributed_product_mode = 'local';

set enable_execute_uncorrelated_subquery=0;
explain SELECT `user_id`
        FROM `dm_mint_crm_creator_stats_df` AS dm_mint_crm_creator_stats_df
        WHERE `date` =
              (
                  SELECT MAX(`date`)
                  FROM `dm_mint_crm_creator_stats_df` AS dm_mint_crm_creator_stats_df
              )
        ORDER BY `followed_counter_td` DESC
                LIMIT 0, 1000
SETTINGS enable_optimize_predicate_expression = 0, distributed_product_mode = 'global', enable_optimize_predicate_expression = 1, distributed_product_mode = 'local';
DROP TABLE IF EXISTS `dm_mint_crm_creator_stats_df`;
