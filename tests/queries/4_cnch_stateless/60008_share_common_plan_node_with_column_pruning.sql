SET enable_share_common_plan_node = 1;
with cte as (select a, b from (select 1 a) cross join (select 2 b)) select c from (select 3 c) cross join (select cte1.a from cte cte1 cross join cte cte2);