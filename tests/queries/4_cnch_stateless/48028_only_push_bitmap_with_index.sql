DROP TABLE if exists aweme_sort_debug_gamma;
CREATE TABLE aweme_sort_debug_gamma 
(`ts` Int64, `uid` Int64, `app_id` Int64, `chn_id` Int64, `rid` String, `data_type` Int64, `ab_vids` Array(Int64) BLOOM, `gid` Int64, `int_params` Map(String, Int64), `float_params` Map(String, Float64), `string_params` Map(String, String))
 ENGINE = CnchMergeTree PARTITION BY toDate(ts) ORDER BY (data_type, app_id, chn_id, uid, gid, intHash64(gid)) SAMPLE BY intHash64(gid) TTL toDate(ts) + toIntervalDay(60)
 SETTINGS index_granularity = 8192, enable_addition_bg_task = 1, cnch_merge_enable_batch_select = 1, cnch_merge_pick_worker_algo = 'RoundRobin', max_addition_bg_task_num = 1000, enable_preload_parts = 1, cnch_merge_max_total_rows_to_merge = 20000000, enable_build_ab_index = 1, max_partition_for_multi_select = 10, gc_remove_part_thread_pool_size = 48, gc_remove_part_batch_size = 5000, cnch_gc_round_robin_partitions_number = 60, cnch_merge_select_nonadjacent_parts = 1, cnch_merge_only_realtime_partition = 1;

insert into aweme_sort_debug_gamma values (1712134865, 1, 1128, 0, 'rid', 0, [1, 2, 3], 100, {'he':1}, {'he':1.2},  {'local_life_deboost_ab_list': '33'});
insert into aweme_sort_debug_gamma values (1712134866, 3, 1128, 0, 'rid', 0, [8586432, 8586431, 3], 100, {'he':1}, {'he':1.2},  {'local_life_deboost_ab_list': '20'});
insert into aweme_sort_debug_gamma values (1712134867, 3, 1128, 0, 'rid', 0, [8586432, 8586431, 3], 100, {'he':1}, {'he':1.2},  {'local_life_deboost_ab_list': '20'});

set enable_optimizer=1;

SELECT
    multiIf(arraySetCheck(splitByChar(',', string_params{'local_life_deboost_ab_list'}), '33'), 'AA', '-1') AS _1700050188505, 
    sum(int_params{'predict_step'}) AS _1700004311268
FROM aweme_sort_debug_gamma
WHERE (chn_id = 0) AND (`app_id` = 1128) 
AND (multiIf(arraySetCheck(ab_vids, 8586431), '对照', arraySetCheck(ab_vids, 8586432), '实验组1', 'else') = '实验组1') AND ((toDate(`ts`) >= '2024-04-03') AND (toDate(`ts`) <= '2024-04-03'))
GROUP BY multiIf(arraySetCheck(splitByChar(',', string_params{'local_life_deboost_ab_list'}), '33'), 'AA', '-1')
LIMIT 100;

explain SELECT
    multiIf(arraySetCheck(splitByChar(',', string_params{'local_life_deboost_ab_list'}), '33'), 'AA', '-1') AS _1700050188505, 
    sum(int_params{'predict_step'}) AS _1700004311268
FROM aweme_sort_debug_gamma
WHERE (chn_id = 0) AND (`app_id` = 1128) 
AND (multiIf(arraySetCheck(ab_vids, 8586431), '对照', arraySetCheck(ab_vids, 8586432), '实验组1', 'else') = '实验组1') AND ((toDate(`ts`) >= '2024-04-03') AND (toDate(`ts`) <= '2024-04-03'))
GROUP BY multiIf(arraySetCheck(splitByChar(',', string_params{'local_life_deboost_ab_list'}), '33'), 'AA', '-1')
LIMIT 100;