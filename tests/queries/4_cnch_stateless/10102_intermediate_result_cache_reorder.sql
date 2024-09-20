set enable_optimizer=1;
set wait_intermediate_result_cache=0;
set enable_optimizer_fallback=0;
set enable_intermediate_result_cache=1;

DROP TABLE if exists reorder_cache_table_all;

CREATE TABLE reorder_cache_table_all(c1 UInt64, c2 String) ENGINE = CnchMergeTree ORDER BY c1;

insert into reorder_cache_table_all values (1, 'a'), (2, 'b'), (2, 'c');

select countDistinct(c2), sum(c1), countDistinct(c1) from reorder_cache_table_all;

select sum(c1), countDistinct(c1), countDistinct(c2), sum(c1) from reorder_cache_table_all settings max_bytes_to_read = 1;
select sum(c1), countDistinct(c2), countDistinct(c1), sum(c1) from reorder_cache_table_all settings max_bytes_to_read = 1;
select countDistinct(c1), countDistinct(c2), sum(c1) from reorder_cache_table_all settings max_bytes_to_read = 1;
select countDistinct(c2), countDistinct(c1), sum(c1) from reorder_cache_table_all settings max_bytes_to_read = 1;
select countDistinct(c2), sum(c1), countDistinct(c1) from reorder_cache_table_all settings max_bytes_to_read = 1;

DROP TABLE reorder_cache_table_all;
