drop table if exists t;

create table t (x UInt64, s String) engine = CnchMergeTree order by x;
INSERT INTO t SELECT
    number,
    if(number < (8129 * 1024), arrayStringConcat(arrayMap(x -> toString(x), range(number % 128)), ' '), '')
FROM numbers_mt((8129 * 1024) * 3) settings max_block_size=10000, max_insert_threads=4;

-- optimize table t;

select count(), sum(length(s)) from t settings max_threads = 3, read_backoff_min_latency_ms = 1, read_backoff_max_throughput = 1000000000, read_backoff_min_interval_between_events_ms = 1, read_backoff_min_events = 1, read_backoff_min_concurrency = 1;
select count(), sum(length(s)) from t settings max_threads = 3, read_backoff_min_latency_ms = 1, read_backoff_max_throughput = 1000000000, read_backoff_min_interval_between_events_ms = 1, read_backoff_min_events = 1, read_backoff_min_concurrency = 1;
select count(), sum(length(s)) from t settings max_threads = 3, read_backoff_min_latency_ms = 1, read_backoff_max_throughput = 1000000000, read_backoff_min_interval_between_events_ms = 1, read_backoff_min_events = 1, read_backoff_min_concurrency = 1;

drop table if exists t;
