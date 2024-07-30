set enable_optimizer=1;
set wait_intermediate_result_cache=0;

DROP TABLE if exists empty_string_cache_table_all;

CREATE TABLE empty_string_cache_table_all(c1 UInt64, c2 String) ENGINE = CnchMergeTree PARTITION BY c1 ORDER BY c1;

SYSTEM STOP MERGES empty_string_cache_table_all;
insert into empty_string_cache_table_all values(1, '');
insert into empty_string_cache_table_all values(1, '');

select c2, sum(c1) from empty_string_cache_table_all group by c2 settings enable_intermediate_result_cache = 1, max_threads = 1;
select c2, sum(c1) from empty_string_cache_table_all group by c2 settings enable_intermediate_result_cache = 1, max_threads = 1;

DROP TABLE empty_string_cache_table_all;
