set enable_optimizer=1;
set wait_intermediate_result_cache=0;
set enable_intermediate_result_cache=1;

DROP TABLE if exists exception_cache_table_all;

CREATE TABLE exception_cache_table_all(c1 UInt64, c2 String) ENGINE = CnchMergeTree ORDER BY c1;

insert into exception_cache_table_all select number, number from numbers(10000);

select c2, sum(c1) from exception_cache_table_all group by c2 settings max_threads = 1, max_untracked_memory=0, max_memory_usage = 1100000;  -- { serverError 241 }

DROP TABLE exception_cache_table_all;
