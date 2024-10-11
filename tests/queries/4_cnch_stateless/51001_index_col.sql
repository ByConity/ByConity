DROP TABLE IF EXISTS test.multi_index_table;

create table test.multi_index_table 
(
    `ts` DateTime64(3),
    `message` String,
    `log` String,
    `int_vid` Array(Int32) BitmapIndex,
    INDEX ts_idx ts TYPE minmax GRANULARITY 4,
    INDEX message_idx message TYPE tokenbf_v1(32768, 2, 0) GRANULARITY 2,
    INDEX log_idx log TYPE inverted GRANULARITY 1
 ) 
ENGINE = CnchMergeTree
PARTITION BY toStartOfInterval(ts, toIntervalHour(12))
ORDER BY ts
SETTINGS index_granularity = 8, enable_nexus_fs = 0;

insert into table test.multi_index_table  values ('2023-10-17 00:11:58.996', 'preload_test1', 'preload_test2', [1, 2, 3, 4, 5])

select message from test.multi_index_table where ts = '2023-10-17 00:11:58.996' and log like 'preload%';
select message from test.multi_index_table where ts = '2023-10-17 00:11:58.996' and log like 'preload%';
select sleepEachRow(3) from system.numbers limit 3 format Null;
select message from test.multi_index_table where ts = '2023-10-17 00:11:58.996' and log like 'preload%' settings disk_cache_mode = 'FORCE_DISK_CACHE';

select 'preload_multi_index_test';

DROP TABLE IF EXISTS test.multi_index_table;
create table test.multi_index_table 
(
    `ts` DateTime64(3),
    `message` String,
    `log` String,
    `int_vid` Array(Int32) BitmapIndex,
    INDEX ts_idx ts TYPE minmax GRANULARITY 4,
    INDEX message_idx message TYPE tokenbf_v1(32768, 2, 0) GRANULARITY 2,
    INDEX log_idx log TYPE inverted GRANULARITY 1
 ) 
ENGINE = CnchMergeTree
PARTITION BY toStartOfInterval(ts, toIntervalHour(12))
ORDER BY ts
SETTINGS index_granularity = 8, enable_nexus_fs = 0;

insert into table test.multi_index_table  values ('2022-10-17 00:11:58.996', 'preload_test1', 'preload_test2', [1, 2, 3, 4, 5])

alter table test.multi_index_table modify setting parts_preload_level = 3;
alter table test.multi_index_table modify setting enable_parts_sync_preload = 1;
alter disk cache preload table test.multi_index_table settings parts_preload_level = 3;
select sleepEachRow(3) from system.numbers limit 3 format Null;
select segments_map from cnch('vw_default', system.part_log) order by event_time desc limit 1;
select message, log, int_vid from test.multi_index_table where ts = '2022-10-17 00:11:58.996' and log like 'preload%' settings disk_cache_mode = 'FORCE_DISK_CACHE';

insert into table test.multi_index_table  values ('2024-10-17 00:11:58.996', 'preload_test3', 'preload_test4', [5, 6, 7, 8, 9]);
select sleepEachRow(3) from system.numbers limit 3 format Null;
select segments_map from cnch('vw_default', system.part_log) order by event_time desc limit 1;
select message, log, int_vid from test.multi_index_table where ts = '2024-10-17 00:11:58.996' and log like 'preload%' settings disk_cache_mode = 'FORCE_DISK_CACHE';
