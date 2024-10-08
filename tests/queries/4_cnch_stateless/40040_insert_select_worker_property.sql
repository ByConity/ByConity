
SET dialect_type='CLICKHOUSE';
SET enable_optimizer = 1;
SET enable_optimizer_fallback = 0;

DROP TABLE IF EXISTS tb_5cf1836f27bf9d05a5b73aee37b52c27;
DROP TABLE IF EXISTS profile_map_tags_test_348590820934;

CREATE TABLE tb_5cf1836f27bf9d05a5b73aee37b52c27 (`suite_dau_7days_avg` Nullable(Int64), `certificate_status` Nullable(String), `tenant_suite_dau` Nullable(Int64), `is_super_admin` Nullable(Int32), `tenant_avg_duration` Nullable(Float64), `ccm_mark` Nullable(Int32), `tag` Nullable(String), `suite_dau_7days_max` Nullable(Int64), `suite_dau_7days_avg_weeklyadd` Nullable(Int64), `tenant_terminal_type` Nullable(String), `tenant_work_avg_dau_7d_diff_rate` Nullable(Float64), `import_ucnt` Nullable(Int64), `user_id` Nullable(String), `vc_mark` Nullable(Int32), `tenant_used_feishu_minutes_storage_rate` Nullable(Float64), `suite_active_days_7days` Nullable(Int64), `tenant_work_dau_1d_diff_rate` Nullable(Float64), `tenant_dau_au_rate` Nullable(Float64), `calendar_mark` Nullable(Int32), `suite_dau_7days_avg_7` Nullable(String), `tenant_used_cloud_storage_rate` Nullable(Float64), `suite_dau_7days_avg_1` Nullable(String), `tenant_mail_storage_g_size_rate` Nullable(Float64), `tenant_active_ucnt_1d_diff_rate` Nullable(Float64), `suite_dau_7days_avg_dailyadd` Nullable(Int64), `base_id` Int64, `date` Nullable(Date), `tenant_active_ucnt_7d_diff_rate` Nullable(Float64), `active_ucnt` Nullable(Int64), `tenant_id` Nullable(String), `tenant_work_avg_duration_7d_diff` Nullable(Float64), `user_maximum` Nullable(Int64), `tenant_work_avg_duration_7d` Nullable(Float64), `is_sub_admin` Nullable(Int32), `tenant_work_avg_duration_1d_diff` Nullable(Float64), `p_date` Date) ENGINE = CnchMergeTree PARTITION BY `p_date` CLUSTER BY `base_id` INTO 40 BUCKETS ORDER BY (`base_id`, intHash64(`base_id`)) SAMPLE BY intHash64(`base_id`) TTL `p_date` + toIntervalDay(90) SETTINGS index_granularity = 8192;
CREATE TABLE profile_map_tags_test_348590820934 (`base_id` Int64, `double_map` Map(String, Float64), `p_date` String, `id_type` UInt16) ENGINE = CnchMergeTree PARTITION BY (toDate(toStartOfDay(`p_date`)), `id_type`) ORDER BY `base_id` SETTINGS index_granularity = 8192;

EXPLAIN INSERT INTO
    profile_map_tags_test_348590820934
SELECT
    *
FROM
    (
        SELECT
            `base_id`,
            map('1427', CAST(`label_value`, 'Float64')) AS double_map,
            '2023-04-26' AS p_date,
            2296 AS id_type
        FROM
            (
                SELECT
                    `base_id`,
                    `label_value`
                FROM
                    (
                        SELECT
                            `base_id` AS base_id,
                            `resultLabel` AS label_value
                        FROM
                            (
                                SELECT
                                    `base_id`,
                                    `date` AS join_left,
                                    max(`tenant_dau_au_rate`) AS resultLabel
                                FROM
                                    `tb_5cf1836f27bf9d05a5b73aee37b52c27`
                                WHERE
                                    (toDate(`p_date`) >= toDate('2023-01-27'))
                                    AND (toDate(`p_date`) <= toDate('2023-04-26'))
                                GROUP BY
                                    `base_id`,
                                    `date`
                            ) AS f
                            INNER JOIN (
                                SELECT
                                    `base_id`,
                                    max(`date`) AS join_right
                                FROM
                                    `tb_5cf1836f27bf9d05a5b73aee37b52c27`
                                WHERE
                                    (toDate(`p_date`) >= toDate('2023-01-27'))
                                    AND (toDate(`p_date`) <= toDate('2023-04-26'))
                                GROUP BY
                                    `base_id`
                            ) AS t ON (`f`.`base_id` = `t`.`base_id`)
                            AND (`f`.`join_left` = `t`.`join_right`)
                    )
                WHERE
                    isNotNull(`label_value`)
            )
    )
LIMIT 1;

INSERT INTO tb_5cf1836f27bf9d05a5b73aee37b52c27(p_date, base_id, tenant_dau_au_rate) VALUES ('2023-04-26', 1, 2.0);
INSERT INTO
    profile_map_tags_test_348590820934
SELECT
    *
FROM
    (
        SELECT
            `base_id`,
            map('1427', CAST(`label_value`, 'Float64')) AS double_map,
            '2023-04-26' AS p_date,
            2296 AS id_type
        FROM
            (
                SELECT
                    `base_id`,
                    `label_value`
                FROM
                    (
                        SELECT
                            `base_id` AS base_id,
                            `resultLabel` AS label_value
                        FROM
                            (
                                SELECT
                                    `base_id`,
                                    `date` AS join_left,
                                    max(`tenant_dau_au_rate`) AS resultLabel
                                FROM
                                    `tb_5cf1836f27bf9d05a5b73aee37b52c27`
                                WHERE
                                    (toDate(`p_date`) >= toDate('2023-01-27'))
                                    AND (toDate(`p_date`) <= toDate('2023-04-26'))
                                GROUP BY
                                    `base_id`,
                                    `date`
                            ) AS f
                            INNER JOIN (
                                SELECT
                                    `base_id`,
                                    max(`date`) AS join_right
                                FROM
                                    `tb_5cf1836f27bf9d05a5b73aee37b52c27`
                                WHERE
                                    (toDate(`p_date`) >= toDate('2023-01-27'))
                                    AND (toDate(`p_date`) <= toDate('2023-04-26'))
                                GROUP BY
                                    `base_id`
                            ) AS t ON (`f`.`base_id` = `t`.`base_id`)
                            AND (`f`.`join_left` = `t`.`join_right`)
                    )
                WHERE
                    isNotNull(`label_value`)
            )
    )
LIMIT 1;

SELECT base_id, double_map FROM profile_map_tags_test_348590820934;
