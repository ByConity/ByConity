drop DATABASE if exists test_48045;
CREATE DATABASE test_48045;

use test_48045;
drop table if exists test_array_join;

set dialect_type = 'CLICKHOUSE';

CREATE TABLE test_array_join
(
    `estimate_count` Nullable(Int64),
    `metric_keys` Array(String),
    `metric_values` Array(Float64),
    `aid` Int64,
    `api_time` Int64,
    `os` String,
    `event_type` String 
)
ENGINE = CnchMergeTree
PARTITION BY toDate(api_time)
ORDER BY (aid, os, event_type, api_time, intHash64(api_time));

insert into test_array_join values(1,['full_network_duration','full_render_duration','full_render_duration_to_start','render_with_cache_duration','base_network_duration','base_render_duration','base_render_duration_to_start'], [671.4760065078735,37.65702247619629,709.1330289840698,0,0,0,0],  36, 1711123200000, 'iOS', 'dcd_page_trace_garage_car_new_detail');

SELECT
    sum(estimate_count) AS sample_count,
    kvs.1 AS metric_key,
    avg(kvs.2) AS avg,
    quantiles(0.25, 0.5, 0.75, 0.9, 0.99)(kvs.2) AS pcts
FROM test_array_join
ARRAY JOIN arrayMap((x, y) -> (x, y), metric_keys, metric_values) AS kvs
WHERE (api_time >= 1710518400000) AND (api_time <= 1711123200000) AND (aid = 36) AND (os = 'iOS') AND (event_type = 'dcd_page_trace_garage_car_new_detail') AND (1 = 1)
GROUP BY kvs.1
order by metric_key;

set enable_optimizer = 1;
Explain verbose = 0, stats = 0
SELECT
    sum(estimate_count) AS sample_count,
    kvs.1 AS metric_key,
    avg(kvs.2) AS avg,
    quantiles(0.25, 0.5, 0.75, 0.9, 0.99)(kvs.2) AS pcts
FROM test_array_join
ARRAY JOIN arrayMap((x, y) -> (x, y), metric_keys, metric_values) AS kvs
WHERE (api_time >= 1710518400000) AND (api_time <= 1711123200000) AND (aid = 36) AND (os = 'iOS') AND (event_type = 'dcd_page_trace_garage_car_new_detail') AND (1 = 1)
GROUP BY kvs.1




