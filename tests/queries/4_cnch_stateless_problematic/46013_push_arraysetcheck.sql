create database if not exists test;

DROP TABLE IF EXISTS aeolus_data_table_5_1383533_prod;

CREATE TABLE aeolus_data_table_5_1383533_prod (`row_id_kmtq3k` Int64, `req_time` Int64, `is_followed` Nullable(UInt8), `staytime3s_t` Nullable(Float64), `uid` Nullable(Int64), `vid` Nullable(String), `app_id` Nullable(String), `actions` Array(Nullable(Int64)), `model_label` Map(String, Float64), `rule_tag` Array(Nullable(String)), `app_name` Nullable(String), `staytime10s_t` Nullable(Float64), `staytime20s_t` Nullable(Float64), `ts` Nullable(Int64), `label` Array(Nullable(Float64)), `model_pred` Map(String, Float64), `rank_channel` Nullable(Int64), `combined_utility_without_boost` Nullable(Float64), `live_category` Nullable(String), `vids` Array(String) BitMapIndex, `gid` Nullable(Int64), `app_version` Nullable(String), `draw` Nullable(UInt8), `os_name` Nullable(String), `is_draw` Nullable(Int64), `douyin_uid` Nullable(Int64), `float_params` Map(String, Float64), `rank` Nullable(Int64), `sati_rank` Nullable(Int64), `live_source` Nullable(Int64), `generate_time` Nullable(Int64), `combined_utility` Nullable(Float64), `int_params` Map(String, Int64), `staytime30s_t` Nullable(Float64), `did` Nullable(String), `gender` Nullable(Float64), `chnid` Nullable(Int64), `req_id` Nullable(String), `ut` Nullable(Int64), `author_id` Nullable(Int64), `string_params` Map(String, String)) ENGINE = CnchMergeTree PARTITION BY toDate(req_time) ORDER BY (req_time, intHash64(`req_time`)) SAMPLE BY intHash64(`req_time`) TTL toDate(req_time) + toIntervalDay(31) SETTINGS index_granularity = 8192, enable_build_ab_index = 1;
EXPLAIN
SELECT
    multiIf(arraySetCheck(vids, '5847335'), 'v1', arraySetCheck(vids, '5847336'), 'v2', arraySetCheck(vids, '5314214'), 'v3', '其它') AS _1700032088007,
    fastAuc2(model_pred{'ppctr'}, CAST(label[23] > 0, 'Nullable(Float64)')) AS _1700032090443
FROM `aeolus_data_table_5_1383533_prod`
WHERE arraySetCheck(vids, ('5847335', '5847336')) AND (multiIf(label[20] > 0, 1, 0) = 1) AND ((toDate(req_time) >= '2023-03-31') AND (toDate(req_time) <= '2023-04-02'))
GROUP BY multiIf(arraySetCheck(vids, '5847335'), 'v1', arraySetCheck(vids, '5847336'), 'v2', arraySetCheck(vids, '5314214'), 'v3', '其它')
LIMIT 1000
SETTINGS enable_optimizer = 1, enable_ab_index_optimization = 1
