drop table if exists aeolus_data_table_8_1759573_prod;
set rewrite_unknown_left_join_identifier = 1;
CREATE TABLE aeolus_data_table_8_1759573_prod
(
    `row_id_kmtq3k` Int64,
    `p_date` Date,
    `payment_num` Nullable(String),
    `payment_creation_time` Nullable(String),
    `payment_apply_time` Nullable(String),
    `process_start_time` Nullable(String),
    `process_approval_end_time` Nullable(String),
    `payment_status_name_zh` Nullable(String),
    `pay_status_name_zh` Nullable(String),
    `payment_applicant_id` Nullable(String),
    `payment_applicant_name_zh` Nullable(String),
    `created_type_name` Nullable(String),
    `supplier_name_zh` Nullable(String),
    `biz_key` Nullable(String),
    `ins_start_time` Nullable(String),
    `ins_end_time` Nullable(String),
    `taskinstance_id` Nullable(String),
    `node_name` Nullable(String),
    `chargeback_node` Nullable(String),
    `reason_name` Nullable(String),
    `reson_level1_name` Nullable(String),
    `reson_level2_name` Nullable(String),
    `reson_level3_name` Nullable(String),
    `assignee_number` Nullable(String),
    `assignee_name` Nullable(String),
    `assignee_team` Nullable(String),
    `assignee_region` Nullable(String),
    `real_chargeback_node` Nullable(Int32),
    `is_one_time_pass` Nullable(String),
    `task_start_time` Nullable(String),
    `task_end_time` Nullable(String),
    `payment_submit_time` Nullable(String),
    `source_system_name` Nullable(String)
)
ENGINE = CnchMergeTree
PARTITION BY p_date
ORDER BY (row_id_kmtq3k, intHash64(row_id_kmtq3k))
SAMPLE BY intHash64(row_id_kmtq3k)
SETTINGS index_granularity = 8192;

SELECT
    _1700039697333,
    table_2._countdistinct_1700039697325 / table_0._countdistinct_1700039697325 AS table_2_countdistinct_1700039697325_store_ratio
FROM
(
    SELECT *
    FROM
    (
        SELECT
            payment_applicant_name_zh AS _1700039697333,
            countDistinct(payment_num) AS _countdistinct_1700039697325
        FROM aeolus_data_table_8_1759573_prod
        GROUP BY payment_applicant_name_zh
    ) AS table_0
)
ALL FULL OUTER JOIN
(
    SELECT
        payment_applicant_name_zh AS _1700039697333,
        countDistinct(payment_num) AS _countdistinct_1700039697325
    FROM aeolus_data_table_8_1759573_prod
    GROUP BY payment_applicant_name_zh
) AS table_2 USING (_1700039697333);

drop table if exists aeolus_data_table_8_1759573_prod;
