set enable_optimizer=1, wait_intermediate_result_cache=0, enable_optimizer_fallback=0, enable_sharding_optimize=1, enable_intermediate_result_cache_ignore_partition_filter=1, enable_partition_filter_push_down=1, mutations_sync = 1;
SET cnch_part_allocation_algorithm = 0;

DROP TABLE if exists cache_table_all;

CREATE TABLE cache_table_all(c1 UInt64, c2 String) ENGINE = CnchMergeTree PARTITION BY c1 ORDER BY c1 settings index_granularity = 1;
system start merges cache_table_all;
system stop merges cache_table_all;

insert into cache_table_all values (1, 'a');
insert into cache_table_all values (2, 'b');

explain select c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1;
select /*test_metric_a*/ c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1;
select c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=0, max_bytes_to_read = 1; -- { serverError 307 }
select /*test_metric_b*/ c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1, max_bytes_to_read = 1;

insert into cache_table_all values (3, 'c');
insert into cache_table_all values (3, 'd');
select c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1, max_bytes_to_read = 1; -- { serverError 307 }
select c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1;

select c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=0, max_bytes_to_read = 1; -- { serverError 307 }
select /*test_metric_d*/ c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1, max_bytes_to_read = 1;

optimize table cache_table_all;
select sleep(3);

select c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1, max_bytes_to_read = 1; -- { serverError 307 }
select /*test_metric_e*/ c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1;
select /*test_metric_f*/ c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1, max_bytes_to_read = 1;

insert into cache_table_all select 4, number from system.numbers limit 10;
select /*test_metric_g*/ c2 from cache_table_all group by c2 order by c2 settings enable_intermediate_result_cache=1, intermediate_result_cache_max_rows = 4;
select c2 from cache_table_all group by c2 order by c2 settings enable_intermediate_result_cache=1, intermediate_result_cache_max_rows = 4, max_bytes_to_read = 1; -- { serverError 307 }
select /*test_metric_h*/ c2 from cache_table_all group by c2 order by c2 settings enable_intermediate_result_cache=1, intermediate_result_cache_max_rows = 4;

truncate table cache_table_all;
insert into cache_table_all select number, number from system.numbers limit 2;
select /*test_metric_i*/ c1, count(*) from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1, max_threads=1;
select /*test_metric_j*/ c1, count(*) from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1, max_threads=1;

-- test distributed perfect sharding
DROP TABLE if exists cache_table_all_sharding;
CREATE TABLE cache_table_all_sharding(c1 UInt64, c2 String) ENGINE = CnchMergeTree PARTITION BY c1 ORDER BY c1 CLUSTER BY c1 INTO 2 BUCKETS settings index_granularity = 1;
insert into cache_table_all_sharding select number, number from system.numbers limit 2;
select c1, count(*) from cache_table_all_sharding group by c1 order by c1 settings enable_intermediate_result_cache=1, distributed_perfect_shard=1, max_bytes_to_read = 1; -- { serverError 307 }
select c1, count(*) from cache_table_all_sharding group by c1 order by c1 settings enable_intermediate_result_cache=1, distributed_perfect_shard=1;
select /*test_metric_k*/ c1, count(*) from cache_table_all_sharding group by c1 order by c1 settings enable_intermediate_result_cache=1, distributed_perfect_shard=1, max_bytes_to_read = 1;

-- test sharding
DROP TABLE if exists cache_table_sharding_all;

CREATE TABLE cache_table_sharding_all(c1 UInt64, c2 String) ENGINE = CnchMergeTree PARTITION BY c1 ORDER BY c1;

insert into cache_table_sharding_all values (1, 'a');
insert into cache_table_sharding_all values (2, 'b');

explain select c1, count(c2) from cache_table_sharding_all group by c1 order by c1 settings enable_intermediate_result_cache=1;
select c1, count(c2) from cache_table_sharding_all group by c1 order by c1 settings enable_intermediate_result_cache=1;
select c1, count(c2) from cache_table_sharding_all group by c1 order by c1 settings enable_intermediate_result_cache=0, max_bytes_to_read = 1; -- { serverError 307 }
select /*test_metric_l*/ c1, count(c2) from cache_table_sharding_all group by c1 order by c1 settings enable_intermediate_result_cache=1, max_bytes_to_read = 1;

-- test partition_filter
explain select c2, count(c1) from cache_table_sharding_all where c1 = 1 group by c2 order by c2 settings enable_intermediate_result_cache=1;
explain select c2, count(c1) from cache_table_sharding_all group by c2 order by c2 settings enable_intermediate_result_cache=1;
explain select c2, count(c1) from cache_table_sharding_all where c1 >= 1 group by c2 order by c2 settings enable_intermediate_result_cache=1;
select c2, count(c1) from cache_table_sharding_all where c1 = 1 group by c2 order by c2 settings enable_intermediate_result_cache=1;
select /*test_metric_m*/ c2, count(c1) from cache_table_sharding_all group by c2 order by c2 settings enable_intermediate_result_cache=1;
select /*test_metric_n*/ c2, count(c1) from cache_table_sharding_all where c1 >= 1 group by c2 order by c2 settings enable_intermediate_result_cache=1;
select /*test_metric_o*/ c2, count(c1) from cache_table_sharding_all where c1 = 1 group by c2 order by c2 settings enable_intermediate_result_cache=1, max_bytes_to_read = 1;
select /*test_metric_p*/ c2, count(c1) from cache_table_sharding_all group by c2 order by c2 settings enable_intermediate_result_cache=1, max_bytes_to_read = 1;
select /*test_metric_q*/ c2, count(c1) from cache_table_sharding_all where c1 >= 1 group by c2 order by c2 settings enable_intermediate_result_cache=1, max_bytes_to_read = 1;

DROP TABLE cache_table_all;
DROP TABLE cache_table_all_sharding;
DROP TABLE cache_table_sharding_all;

DROP TABLE if exists cache_table_sharding_all;

SYSTEM FLUSH LOGS;
select sleep(3);
select sleep(3);
select sleep(3);
select sleep(3);
select sleep(3);

SELECT
    metric,
    sum(Hits),
    sum(Misses),
    sum(Refuses),
    sum(Wait),
    sum(Uncompleted),
    sum(ReadBytes),
    sum(WriteBytes)
FROM
(
    SELECT
        a.metric metric,
        COALESCE(b.ProfileEvents{'IntermediateResultCacheHits'}, 0) AS Hits,
        COALESCE(b.ProfileEvents{'IntermediateResultCacheMisses'}, 0) AS Misses,
        COALESCE(b.ProfileEvents{'IntermediateResultCacheRefuses'}, 0) AS Refuses,
        COALESCE(b.ProfileEvents{'IntermediateResultCacheWait'}, 0) AS Wait,
        COALESCE(b.ProfileEvents{'IntermediateResultCacheUncompleted'}, 0) AS Uncompleted,
        COALESCE(b.ProfileEvents{'IntermediateResultCacheReadBytes'}, 0) AS ReadBytes,
        COALESCE(b.ProfileEvents{'IntermediateResultCacheWriteBytes'}, 0) AS WriteBytes
    FROM (select substring(query, 10, 13) metric, event_time, event_date, initial_query_id from cnch('server', system.query_log) WHERE (event_date = today()) AND (query NOT ILIKE '%system%') AND (query ILIKE '%test_metric_%') AND (type = 1) ORDER BY event_time DESC limit 16) AS a
    INNER JOIN cnch('vw_default', system.query_log) AS b USING (event_date, initial_query_id)
    WHERE (Hits + Misses + Refuses + Wait + Uncompleted + ReadBytes + WriteBytes) > 0
)
GROUP BY metric
ORDER BY metric ASC;
