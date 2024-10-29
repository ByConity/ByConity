create database if not exists my_test;
USE my_test;

DROP TABLE IF EXISTS my_test.dwd_abtest_vid_log_realtime_aweme;

CREATE TABLE my_test.dwd_abtest_vid_log_realtime_aweme (
    `unique_str` String,
    `split_key` Int64,
    `timestamp` Int64,
    `app_name` String,
    `did` Int64,
    `uid` Int64,
    `split_type` Int64,
    `source_type` String,
    `is_dau_user` Int8,
    `process_timestamp` String,
    `action_type` Int8,
    `app_id` Int32,
    `uuid` String
) ENGINE = CnchMergeTree PARTITION BY (
    toDate(timestamp)
)
cluster by uid into 128 buckets
ORDER BY(`split_key`, `split_type`, intHash64(split_key)) 
UNIQUE KEY sipHash64(unique_str) SAMPLE BY intHash64(split_key) TTL toDate(timestamp) + toIntervalDay(7) 
SETTINGS index_granularity = 8192;

set enable_optimizer=1;

explain select split_key, app_id, max(timestamp) update_time
        from my_test.dwd_abtest_vid_log_realtime_aweme
        where (toDateTime(`timestamp`)) BETWEEN ('2023-08-13 20:12:15') AND ('2023-08-14 00:41:00')
        group by app_id, split_key
        SETTINGS distributed_group_by_no_merge = 1;

alter table my_test.dwd_abtest_vid_log_realtime_aweme modify setting cluster_by_hint='split_key';

explain select split_key, app_id, max(timestamp) update_time
        from my_test.dwd_abtest_vid_log_realtime_aweme
        where (toDateTime(`timestamp`)) BETWEEN ('2023-08-13 20:12:15') AND ('2023-08-14 00:41:00')
        group by app_id, split_key
        SETTINGS distributed_group_by_no_merge = 1;

alter table my_test.dwd_abtest_vid_log_realtime_aweme modify setting cluster_by_hint='';

DROP TABLE IF EXISTS my_test.dwd_abtest_vid_log_realtime_aweme;