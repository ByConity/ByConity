set enable_optimizer=1, enable_intermediate_result_cache=1, wait_intermediate_result_cache=0, database_atomic_wait_for_drop_and_detach_synchronously=1;
set cnch_part_allocation_algorithm = 0;
set mutations_sync = 1;

DROP TABLE if exists cache_unique_table;
DROP TABLE if exists cache_non_unique_table;

CREATE TABLE cache_unique_table
(
    `event_time` DateTime,
    `uk` UInt64,
    `c1` UInt64
)
ENGINE = CnchMergeTree
PARTITION BY toDate(event_time)
ORDER BY (c1)
UNIQUE KEY uk;

SYSTEM START MERGES cache_unique_table;

insert into cache_unique_table values ('2020-10-26 23:40:00', 101, 1);
insert into cache_unique_table values ('2020-10-27 23:40:00', 102, 2);
insert into cache_unique_table values ('2020-10-28 23:40:00', 103, 3);
insert into cache_unique_table values ('2020-10-29 23:40:00', 104, 4);

select /*test_modification_a*/ c1 from cache_unique_table group by c1 order by c1;
select /*test_modification_b*/ c1 from cache_unique_table group by c1 order by c1 settings max_bytes_to_read = 1;

select sleep(1); -- FIXME: because precision of modification_time in owner_info
insert into cache_unique_table values ('2020-10-26 23:40:00', 101, 5);
insert into cache_unique_table values ('2020-10-27 23:40:00', 102, 6);
insert into cache_unique_table values ('2020-10-28 23:40:00', 103, 7);
insert into cache_unique_table values ('2020-10-29 23:40:00', 104, 8);
select /*test_modification_c*/ c1 from cache_unique_table group by c1 order by c1;
select /*test_modification_d*/ c1 from cache_unique_table group by c1 order by c1;

insert into cache_unique_table (*, _delete_flag_) SELECT *, 1 as _delete_flag_ FROM cache_unique_table where uk = 101;
select /*test_modification_e*/ c1 from cache_unique_table group by c1 order by c1;
select /*test_modification_f*/ c1 from cache_unique_table group by c1 order by c1;

CREATE TABLE cache_non_unique_table
(
    `event_time` DateTime,
    `uk` UInt64,
    `c1` UInt64
)
ENGINE = CnchMergeTree
PARTITION BY toDate(event_time)
ORDER BY (c1);

SYSTEM START MERGES cache_non_unique_table;

insert into cache_non_unique_table values ('2020-10-26 23:40:00', 101, 1);
insert into cache_non_unique_table values ('2020-10-27 23:40:00', 102, 2);
insert into cache_non_unique_table values ('2020-10-28 23:40:00', 103, 3);
insert into cache_non_unique_table values ('2020-10-29 23:40:00', 104, 4);

select /*test_modification_g*/ c1 from cache_non_unique_table group by c1 order by c1;
select /*test_modification_h*/ c1 from cache_non_unique_table group by c1 order by c1 settings max_bytes_to_read = 1;

select sleep(1); -- FIXME: because precision of modification_time in owner_info

ALTER TABLE cache_non_unique_table DELETE WHERE uk = 102;
select /*test_modification_i*/ c1 from cache_non_unique_table group by c1 order by c1;
select /*test_modification_j*/ c1 from cache_non_unique_table group by c1 order by c1;
select c1 from cache_non_unique_table group by c1 order by c1 settings enable_intermediate_result_cache=0 ;

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
        FROM (select substring(query, 10, 19) metric, event_time, event_date, initial_query_id from cnch('server', system.query_log)
              WHERE (event_date = today()) AND (query NOT ILIKE '%system%') AND (query ILIKE '%test_modification_%') AND (type = 1) ORDER BY event_time DESC limit 10) AS a
                 INNER JOIN cnch('vw_default', system.query_log) AS b USING (event_date, initial_query_id)
        WHERE (Hits + Misses + Refuses + Wait + Uncompleted + ReadBytes + WriteBytes) > 0
    )
GROUP BY metric
ORDER BY metric ASC;
