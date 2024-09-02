set enable_optimizer=1;
set enable_intermediate_result_cache=1;
set wait_intermediate_result_cache=0;

DROP TABLE if exists join_cache_table_all;

CREATE TABLE join_cache_table_all(c1 UInt64, c2 String) ENGINE = CnchMergeTree PARTITION BY c1 ORDER BY c1 settings index_granularity = 1;
system start merges join_cache_table_all;
system stop merges join_cache_table_all;

insert into join_cache_table_all values (1, 'a');
insert into join_cache_table_all values (2, 'b');

explain select a.c1 from join_cache_table_all a, join_cache_table_all b group by a.c1 order by a.c1 settings enable_join_intermediate_result_cache=0;
explain select a.c1 from join_cache_table_all a, join_cache_table_all b group by a.c1 order by a.c1 settings enable_join_intermediate_result_cache=1;
select a.c1 from join_cache_table_all a, join_cache_table_all b group by a.c1 order by a.c1 settings enable_join_intermediate_result_cache=1;
select a.c1 from join_cache_table_all a, join_cache_table_all b group by a.c1 order by a.c1 settings enable_join_intermediate_result_cache=0, max_bytes_to_read = 1; -- { serverError 307 }
select a.c1 from join_cache_table_all a, join_cache_table_all b group by a.c1 order by a.c1 settings enable_join_intermediate_result_cache=1, max_rows_to_read = 2;

insert into join_cache_table_all values (3, 'c');
select a.c1 from join_cache_table_all a, join_cache_table_all b group by a.c1 order by a.c1 settings enable_join_intermediate_result_cache=1, max_bytes_to_read = 1; -- { serverError 307 }
select a.c1 from join_cache_table_all a, join_cache_table_all b group by a.c1 order by a.c1 settings enable_join_intermediate_result_cache=1, max_rows_to_read = 3;

optimize table join_cache_table_all;
select a.c1 from join_cache_table_all a, join_cache_table_all b group by a.c1 order by a.c1 settings enable_join_intermediate_result_cache=1, max_bytes_to_read = 1; -- { serverError 307 }
select a.c1 from join_cache_table_all a, join_cache_table_all b group by a.c1 order by a.c1 settings enable_join_intermediate_result_cache=1;
select a.c1 from join_cache_table_all a, join_cache_table_all b group by a.c1 order by a.c1 settings enable_join_intermediate_result_cache=1, max_rows_to_read = 3;

DROP TABLE join_cache_table_all;
