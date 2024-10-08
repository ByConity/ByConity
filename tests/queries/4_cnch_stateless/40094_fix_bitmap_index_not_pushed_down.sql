CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS t40094_bitmap_index;

CREATE TABLE t40094_bitmap_index
(
    `row_id_kmtq3k` Int64,
    `req_time` Int64,
    `is_followed` Nullable(UInt8),
    `staytime3s_t` Nullable(Float64),
    `uid` Nullable(Int64),
    `vid` Nullable(String),
    `app_id` Nullable(String),
    `actions` Array(Nullable(Int64)),
    `model_label` Map(String, Float64),
    `rule_tag` Array(Nullable(String)),
    `app_name` Nullable(String),
    `staytime10s_t` Nullable(Float64),
    `staytime20s_t` Nullable(Float64),
    `ts` Nullable(Int64),
    `label` Array(Nullable(Float64)),
    `model_pred` Map(String, Float64),
    `rank_channel` Nullable(Int64),
    `combined_utility_without_boost` Nullable(Float64),
    `live_category` Nullable(String),
    `vids` Array(String) BitmapIndex,
    `gid` Nullable(Int64),
    `app_version` Nullable(String),
    `draw` Nullable(UInt8),
    `os_name` Nullable(String),
    `is_draw` Nullable(Int64),
    `douyin_uid` Nullable(Int64),
    `float_params` Map(String, Float64),
    `rank` Nullable(Int64),
    `sati_rank` Nullable(Int64),
    `live_source` Nullable(Int64),
    `generate_time` Nullable(Int64),
    `combined_utility` Nullable(Float64),
    `int_params` Map(String, Int64),
    `staytime30s_t` Nullable(Float64),
    `did` Nullable(String),
    `gender` Nullable(Float64),
    `chnid` Nullable(Int64),
    `req_id` Nullable(String),
    `ut` Nullable(Int64),
    `author_id` Nullable(Int64),
    `string_params` Map(String, String)
)
ENGINE = CnchMergeTree
PARTITION BY toDate(req_time)
ORDER BY (req_time, intHash64(req_time))
SAMPLE BY intHash64(req_time)
TTL toDate(req_time) + toIntervalDay(31)
SETTINGS  index_granularity = 8192, enable_build_ab_index = 1;

EXPLAIN
SELECT
    if(arraySetCheck(vids, '8683112'), '8683112', if(arraySetCheck(vids, '8683113'), '8683113', 'other')) AS _1700046658469,
    sum(multiIf((label[31]) > 0, 1, 0)) AS _sum_1700046657800,
    sum(label[32]) AS _sum_1700046657910,
    sum(multiIf((label[1]) >= 1, 1, 0)) AS _sum_1700046657811,
    count(1) AS _1700031876736,
    countDistinct(uid) AS _countdistinct_1700031876715,
    avg(model_label{'staytime'}) AS _avg_1700031876697_4700826bc6c9c490e9a7756937dd1f6e,
    CAST(avg(multiIf(model_label{'staytime'} >= 30000, 1, 0)), 'Nullable(Float64)') AS _1700031897102,
    CAST(avg(multiIf(model_label{'staytime'} >= 60000, 1, 0)), 'Nullable(Float64)') AS _1700047638968
FROM t40094_bitmap_index
WHERE ((toDateTime(req_time) >= '2024-04-12 00:00:00') AND (toDateTime(req_time) <= '2024-04-12 09:59:59')) AND (app_id = '13') AND (string_params{'chnid'} = '94349560412') AND (if(arraySetCheck(vids, '8683112'), '8683112', if(arraySetCheck(vids, '8683113'), '8683113', 'other')) IN ('8683112', '8683113')) AND (arrayJoin(assumeNotNull(split(string_params{'g_recall_reasons'}, ',') AS src)) = 'aaebvr') AND ((toDate(req_time) >= '2024-04-12') AND (toDate(req_time) <= '2024-04-12'))
GROUP BY if(arraySetCheck(vids, '8683112'), '8683112', if(arraySetCheck(vids, '8683113'), '8683113', 'other'))
LIMIT 1000
SETTINGS max_memory_usage = 21474836480, max_bytes_to_read = 2147483648000, max_execution_time = 100, max_bytes_to_read_local = 32212254720, enable_optimizer = 1, enable_ab_index_optimization = 1;

DROP TABLE IF EXISTS t40094_bitmap_index;
