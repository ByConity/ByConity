set enable_optimizer=1;
set enable_intermediate_result_cache=1;
set wait_intermediate_result_cache=0;
set enable_optimizer_fallback=0;
-- set enable_expand_distinct_optimization=1;
set enable_mark_distinct_optimzation=0;

DROP TABLE if exists cache_table_all;

CREATE TABLE cache_table_all(c1 UInt64, c2 String, c3 Int32) ENGINE = CnchMergeTree PARTITION BY c1 ORDER BY c1;

insert into cache_table_all values (1, 'a', 1);
insert into cache_table_all values (2, 'b', 1);
insert into cache_table_all values (3, 'c', 1);

select c1, count(distinct c2) / sum(c3) from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1;
select c1, count(distinct c2) / sum(c3) from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1, max_bytes_to_read=1;
