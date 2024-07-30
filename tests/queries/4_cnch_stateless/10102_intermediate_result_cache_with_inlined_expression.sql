DROP TABLE IF EXISTS ad_abtest_union_preprocess_hl;

CREATE TABLE ad_abtest_union_preprocess_hl (
    `access_method` Int64,
    `active_cnt` Int32,
    `ad_id` String,
    `ad_package_name` String,
    `ad_sdk_version` String,
    `ad_slot_type` String,
    `adv_target_pay_ratio` Float64,
    `adv_value` Float64,
    `advertiser_id` String,
    `at` Int64,
    `bid` Float64,
    `bid_without_ug` Float64,
    `brier_score_sum` Float64,
    `cache_hit` String,
    `cid_risk_labels` Array (Int64) BitmapIndex,
    `classify` Int64,
    `click` Int32,
    `click_cnt` String,
    `click_fraud_cnt` Int32,
    `click_start` Int32,
    `click_start_detail` Int32,
    `content_type` Int64,
    `convert` Int32,
    `convert_cnt` String,
    `convert_q` String,
    `cost` Float64,
    `cost_without_ug` Float64,
    `cpm_target` UInt8,
    `creative_id` Int64,
    `customer_id` String,
    `cypher_random_send` Int32,
    `deep_adv_value` Float64,
    `deep_bid_type` Int64,
    `deep_convert` Int32,
    `deep_external_action` Int64,
    `deepconv_retention` Float64,
    `did` Int64,
    `double_params` Map (String, Float64),
    `download_finish` Int32,
    `download_start` Int32,
    `dsp_cost` Float64,
    `dsp_id` Int64,
    `dsp_price` Float64,
    `dsp_profit_true` Float64,
    `dynamic_ptpl_id` String,
    `ecpm` Float64,
    `ectr` Float64,
    `ectr_click_cnt` Int32,
    `ecvr` Float64,
    `endcard_layout_id` Int64,
    `engine_cluster` String,
    `estimated_adv_value` Float64,
    `event_based_revenue` Float64,
    `external_action` String,
    `flow_control_mode` Int64,
    `fraud_cnt` Int32,
    `image_mode` Int64,
    `industry1_v3` Int64,
    `install_finish` Int32,
    `is_real_playable` Int64,
    `landing_type` Int64,
    `last_ecpm` Float64,
    `long_params` Map (String, Int64),
    `news_pos` String,
    `next_day_open_cnt` Int32,
    `orientation` String,
    `origin_type` Int32,
    `ot_ts` Int64,
    `p_pay_sum` Float64,
    `platform` Int64,
    `pricing` Int64,
    `rank_bid` Float64,
    `req_id` String,
    `rim` Int64,
    `rit` Int64,
    `ritshow_cnt` String,
    `send` Int32,
    `shadow_strategy_version` String,
    `show` Int32,
    `show_fraud_cnt` Int32,
    `site_type` String,
    `smart_drop_level` String,
    `sorted_ecpm` Float64,
    `source_type` String,
    `string_params` Map (String, String),
    `total_latency` Int64,
    `total_latency_cnt` Int32,
    `total_pay_cnt` Int32,
    `ttdsp_adx_index` Int64,
    `two_days_active_pay_cnt` Int32,
    `union_cold_start_exp_ad_flag` String,
    `union_imei` String,
    `union_predict_cluster` String,
    `union_predict_latency` Int64,
    `union_predict_latency_cnt` Int32,
    `union_predict_pay_ratio_predict` Float64,
    `union_predict_r2w_score` Float64,
    `union_special` Int64,
    `union_template_id` String,
    `unique_pay_cnt` Int32,
    `uroi_type` String,
    `valid_dsp_price` Float64,
    `vendor` String,
    `vid_array` Array (Int32) BLOOM,
    `win_cnt_union` Int32
) ENGINE = CnchMergeTree ()
PARTITION BY
    (toDate (at), toDate (ot_ts))
ORDER BY
    (
        external_action,
        ttdsp_adx_index,
        landing_type,
        pricing,
        rit,
        customer_id,
        advertiser_id,
        ad_id,
        ot_ts,
        cityHash64 (ad_id)
    ) SAMPLE BY cityHash64 (ad_id) TTL toDate (at) + toIntervalDay (14) SETTINGS index_granularity = 8192;


SELECT
    toDate(ot_ts),
    long_params{'union_media_id'},
    sum(send),
    sum(show),
    sum(cost)
FROM ad_abtest_union_preprocess_hl
WHERE (long_params{'union_media_id'} IN (57784)) AND arraySetCheck(cid_risk_labels, 801053) AND (toInt64(customer_id) IN (3098607)) AND (toDate(ot_ts) >= '2024-03-14') AND (toDate(ot_ts) <= '2024-03-15')
AND (toDateTime(ot_ts) >= '2024-03-14 00:00:00') AND (toDateTime(ot_ts) <= '2024-03-15 23:59:59')
GROUP BY
    toDate(ot_ts),
    long_params{'union_media_id'}
LIMIT 0, 1000
SETTINGS enable_optimizer = 1, enable_intermediate_result_cache = 1, enable_join_intermediate_result_cache = 1;

SELECT
    toDate(ot_ts),
    long_params{'union_media_id'},
    sum(send),
    sum(show),
    sum(cost)
FROM ad_abtest_union_preprocess_hl
WHERE (long_params{'union_media_id'} IN (57784)) AND arraySetCheck(cid_risk_labels, 801053) AND (toInt64(customer_id) IN (3098607)) AND (toDate(ot_ts) >= '2024-03-14') AND (toDate(ot_ts) <= '2024-03-15')
AND (toDateTime(ot_ts) >= '2024-03-14 00:00:00') AND (toDateTime(ot_ts) <= '2024-03-15 23:59:59')
GROUP BY
    toDate(ot_ts),
    long_params{'union_media_id'}
LIMIT 0, 1000
SETTINGS enable_optimizer = 1, enable_intermediate_result_cache = 1, enable_join_intermediate_result_cache = 1;
