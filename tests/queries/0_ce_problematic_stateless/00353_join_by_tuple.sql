set any_join_distinct_right_table_keys = 1;
SET enable_optimizer = 0; -- TODO: semi/anti join is not supported by optimizer
select a from (select (1, 2) as a) js1 any inner join (select (1, 2) as a) js2 using a;
