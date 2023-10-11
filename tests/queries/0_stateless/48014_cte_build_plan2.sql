set dialect_type='ANSI';

CREATE TABLE dwd_wmup_llpt_organ_dict
(
    `organ_id` String NOT NULL,
    `organ_name` String NULL,
    `organ_level` Int32 NULL,
    `organ_id_1` String NULL,
    `organ_name_1` String NULL,
    `organ_id_2` String NULL,
    `organ_name_2` String NULL,
    `organ_id_3` String NULL,
    `organ_name_3` String NULL,
    `organ_id_4` String NULL,
    `organ_name_4` String NULL,
    `organ_id_5` String NULL,
    `organ_name_5` String NULL,
    `snapshot_gen_time` Int64 NULL
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'dwd_wmup_llpt_organ_dict_local', cityHash64(organ_id));

CREATE TABLE dwd_wmup_llpt_sign_detail
(
    `customer_sign_time` DateTime NOT NULL,
    `mobile` String NOT NULL,
    `activity_type` String NOT NULL,
    `plat_type` String NOT NULL,
    `agent_grade_desc` String NOT NULL,
    `organ_id_1` String NULL,
    `organ_name_1` String NULL,
    `organ_id_2` String NULL,
    `organ_name_2` String NULL,
    `organ_id_3` String NULL,
    `organ_name_3` String NULL,
    `organ_id_4` String NULL,
    `organ_name_4` String NULL,
    `organ_id_5` String NULL,
    `organ_name_5` String NULL,
    `activity_begin_time` String NULL,
    `activity_end_time` String NULL,
    `activity_name` String NULL,
    `act_no` String NULL,
    `activity_address` String NULL,
    `cust_name` String NULL,
    `mask_cust_name` String NULL,
    `mask_mobile` String NULL,
    `air_id` String NULL,
    `core_cust_id` String NULL,
    `if_new_cust` Int32 NULL,
    `is_agent_mobile` String NULL,
    `mobile_agent_code` String NULL,
    `sign_id` Int64 NULL,
    `wm_sign_order_mon` Int32 NULL,
    `wm_sign_order_year` Int32 NULL,
    `llpt_sign_order_mon` Int32 NULL,
    `llpt_sign_order_year` Int32 NULL,
    `agent_code` String NULL,
    `agent_grade` String NULL,
    `agent_name` String NULL,
    `ag_organ_id_2` String NULL,
    `ag_organ_name_2` String NULL,
    `data_flag` String NULL,
    `p_key` String NULL,
    `p_key2` String NULL,
    `activity_type_name` String NULL,
    `activity_parent_label_name` String NULL,
    `snapshot_gen_time` Int64 NULL
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'dwd_wmup_llpt_sign_detail_local', cityHash64(mobile));



CREATE TABLE dwd_wmup_llpt_sign_detail_with_convert
(
    `customer_sign_time` DateTime NOT NULL,
    `mobile` String NOT NULL,
    `activity_type` String NOT NULL,
    `plat_type` String NOT NULL,
    `agent_grade_desc` String NOT NULL,
    `organ_id_1` String NULL,
    `organ_name_1` String NULL,
    `organ_id_2` String NULL,
    `organ_name_2` String NULL,
    `organ_id_3` String NULL,
    `organ_name_3` String NULL,
    `organ_id_4` String NULL,
    `organ_name_4` String NULL,
    `organ_id_5` String NULL,
    `organ_name_5` String NULL,
    `cust_name` String NULL,
    `mask_cust_name` String NULL,
    `mask_mobile` String NULL,
    `if_new_cust` Int32 NULL,
    `is_agent_mobile` String NULL,
    `hold_customer_ind` String NULL,
    `activity_begin_time` DateTime NULL,
    `activity_end_time` DateTime NULL,
    `activity_name` String NULL,
    `act_no` String NULL,
    `activity_address` String NULL,
    `mobile_agent_code` String NULL,
    `sign_id` Int64 NULL,
    `agent_code` String NULL,
    `agent_grade` String NULL,
    `agent_name` String NULL,
    `ag_organ_id_2` String NULL,
    `ag_organ_name_2` String NULL,
    `data_flag` String NULL,
    `p_key` String NULL,
    `convert_organ_id_2` String NULL,
    `convert_organ_name_2` String NULL,
    `convert_organ_id_3` String NULL,
    `convert_organ_name_3` String NULL,
    `convert_organ_id_4` String NULL,
    `convert_organ_name_4` String NULL,
    `convert_organ_id_5` String NULL,
    `convert_organ_name_5` String NULL,
    `convert_agent_code` String NULL,
    `convert_agent_grade` String NULL,
    `convert_agent_grade_desc` String NULL,
    `accept_date` DateTime NULL,
    `long_pol_flag` Int32 NULL,
    `policy_code` String NULL,
    `convert_prem` Decimal(38, 6) NULL,
    `convert_value` Int64 NULL,
    `m_convert_ind` Int32 NULL,
    `30d_convert_ind` Int32 NULL,
    `y_convert_ind` Int32 NULL,
    `m_plc_period_prem` Decimal(38, 6) NULL,
    `m_plc_prem_value` Int64 NULL,
    `30d_plc_period_prem` Decimal(38, 6) NULL,
    `30d_plc_prem_value` Int64 NULL,
    `y_plc_period_prem` Decimal(38, 6) NULL,
    `y_plc_prem_value` Int64 NULL,
    `pk_col` String NULL,
    `activity_type_name` String NULL,
    `activity_parent_label_name` String NULL,
    `snapshot_gen_time` Int64 NULL
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'dwd_wmup_llpt_sign_detail_with_convert_local', cityHash64(mobile));


CREATE TABLE dwd_wmup_llpt_organ_dict_local
(
    `organ_id` String NOT NULL,
    `organ_name` String NULL,
    `organ_level` Int32 NULL,
    `organ_id_1` String NULL,
    `organ_name_1` String NULL,
    `organ_id_2` String NULL,
    `organ_name_2` String NULL,
    `organ_id_3` String NULL,
    `organ_name_3` String NULL,
    `organ_id_4` String NULL,
    `organ_name_4` String NULL,
    `organ_id_5` String NULL,
    `organ_name_5` String NULL,
    `snapshot_gen_time` Int64 NULL
)
ENGINE = MergeTree
ORDER BY organ_id
SETTINGS index_granularity = 8192;


CREATE TABLE dwd_wmup_llpt_sign_detail_local
(
    `customer_sign_time` DateTime NOT NULL,
    `mobile` String NOT NULL,
    `activity_type` String NOT NULL,
    `plat_type` String NOT NULL,
    `agent_grade_desc` String NOT NULL,
    `organ_id_1` String NULL,
    `organ_name_1` String NULL,
    `organ_id_2` String NULL,
    `organ_name_2` String NULL,
    `organ_id_3` String NULL,
    `organ_name_3` String NULL,
    `organ_id_4` String NULL,
    `organ_name_4` String NULL,
    `organ_id_5` String NULL,
    `organ_name_5` String NULL,
    `activity_begin_time` String NULL,
    `activity_end_time` String NULL,
    `activity_name` String NULL,
    `act_no` String NULL,
    `activity_address` String NULL,
    `cust_name` String NULL,
    `mask_cust_name` String NULL,
    `mask_mobile` String NULL,
    `air_id` String NULL,
    `core_cust_id` String NULL,
    `if_new_cust` Int32 NULL,
    `is_agent_mobile` String NULL,
    `mobile_agent_code` String NULL,
    `sign_id` Int64 NULL,
    `wm_sign_order_mon` Int32 NULL,
    `wm_sign_order_year` Int32 NULL,
    `llpt_sign_order_mon` Int32 NULL,
    `llpt_sign_order_year` Int32 NULL,
    `agent_code` String NULL,
    `agent_grade` String NULL,
    `agent_name` String NULL,
    `ag_organ_id_2` String NULL,
    `ag_organ_name_2` String NULL,
    `data_flag` String NULL,
    `p_key` String NULL,
    `p_key2` String NULL,
    `activity_type_name` String NULL,
    `activity_parent_label_name` String NULL,
    `snapshot_gen_time` Int64 NULL
)
ENGINE = MergeTree
ORDER BY (customer_sign_time, mobile, activity_type, plat_type, agent_grade_desc)
SETTINGS index_granularity = 8192;


CREATE TABLE dwd_wmup_llpt_sign_detail_with_convert_local
(
    `customer_sign_time` DateTime NOT NULL,
    `mobile` String NOT NULL,
    `activity_type` String NOT NULL,
    `plat_type` String NOT NULL,
    `agent_grade_desc` String NOT NULL,
    `organ_id_1` String NULL,
    `organ_name_1` String NULL,
    `organ_id_2` String NULL,
    `organ_name_2` String NULL,
    `organ_id_3` String NULL,
    `organ_name_3` String NULL,
    `organ_id_4` String NULL,
    `organ_name_4` String NULL,
    `organ_id_5` String NULL,
    `organ_name_5` String NULL,
    `cust_name` String NULL,
    `mask_cust_name` String NULL,
    `mask_mobile` String NULL,
    `if_new_cust` Int32 NULL,
    `is_agent_mobile` String NULL,
    `hold_customer_ind` String NULL,
    `activity_begin_time` DateTime NULL,
    `activity_end_time` DateTime NULL,
    `activity_name` String NULL,
    `act_no` String NULL,
    `activity_address` String NULL,
    `mobile_agent_code` String NULL,
    `sign_id` Int64 NULL,
    `agent_code` String NULL,
    `agent_grade` String NULL,
    `agent_name` String NULL,
    `ag_organ_id_2` String NULL,
    `ag_organ_name_2` String NULL,
    `data_flag` String NULL,
    `p_key` String NULL,
    `convert_organ_id_2` String NULL,
    `convert_organ_name_2` String NULL,
    `convert_organ_id_3` String NULL,
    `convert_organ_name_3` String NULL,
    `convert_organ_id_4` String NULL,
    `convert_organ_name_4` String NULL,
    `convert_organ_id_5` String NULL,
    `convert_organ_name_5` String NULL,
    `convert_agent_code` String NULL,
    `convert_agent_grade` String NULL,
    `convert_agent_grade_desc` String NULL,
    `accept_date` DateTime NULL,
    `long_pol_flag` Int32 NULL,
    `policy_code` String NULL,
    `convert_prem` Decimal(38, 6) NULL,
    `convert_value` Int64 NULL,
    `m_convert_ind` Int32 NULL,
    `30d_convert_ind` Int32 NULL,
    `y_convert_ind` Int32 NULL,
    `m_plc_period_prem` Decimal(38, 6) NULL,
    `m_plc_prem_value` Int64 NULL,
    `30d_plc_period_prem` Decimal(38, 6) NULL,
    `30d_plc_prem_value` Int64 NULL,
    `y_plc_period_prem` Decimal(38, 6) NULL,
    `y_plc_prem_value` Int64 NULL,
    `pk_col` String NULL,
    `activity_type_name` String NULL,
    `activity_parent_label_name` String NULL,
    `snapshot_gen_time` Int64 NULL
)
ENGINE = MergeTree
ORDER BY (customer_sign_time, mobile, activity_type, plat_type, agent_grade_desc)
SETTINGS index_granularity = 8192;

INSERT INTO dwd_wmup_llpt_sign_detail (mobile, customer_sign_time, activity_type, plat_type, agent_grade_desc) SELECT
    toString(number) AS mobile,
    now() AS customer_sign_time,
    toString(intDiv(number, 500000)) AS activity_type,
    toString(intDiv(number, 500000)) AS plat_type,
    toString(intDiv(number, 50000)) AS agent_grade_desc
FROM numbers(1000000);


INSERT INTO dwd_wmup_llpt_sign_detail_with_convert_local (mobile, customer_sign_time, activity_type, plat_type, agent_grade_desc) SELECT
    toString(number) AS mobile,
    now() AS customer_sign_time,
    toString(intDiv(number, 500000)) AS activity_type,
    toString(intDiv(number, 500000)) AS plat_type,
    toString(intDiv(number, 50000)) AS agent_grade_desc
FROM numbers(1000000);

create stats all Format Null;

WITH
    act_type_dict AS
    (
        SELECT
            '0' AS act_type,
            '机构活动' AS act_type_name
        UNION ALL
        SELECT
            '1' AS act_type,
            '家办活动' AS act_type_name
        UNION ALL
        SELECT
            '2' AS act_type,
            '权V认证-第一季' AS act_type_name
        UNION ALL
        SELECT
            '22' AS act_type,
            '权V认证-第二季-健康关爱季' AS act_type_name
        UNION ALL
        SELECT
            '23' AS act_type,
            '权V认证第三季' AS act_type_name
        UNION ALL
        SELECT
            '24' AS act_type,
            '权V认证第四季' AS act_type_name
        UNION ALL
        SELECT
            '51' AS act_type,
            '美好畅享会' AS act_type_name
        UNION ALL
        SELECT
            '31' AS act_type,
            '权V认证-银保专属' AS act_type_name
        UNION ALL
        SELECT
            '41' AS act_type,
            '星海服务会' AS act_type_name
        UNION ALL
        SELECT
            'ACTIVE0003' AS act_type,
            '客户服务' AS act_type_name
        UNION ALL
        SELECT
            'ACTIVE0001' AS act_type,
            '养老社区活动' AS act_type_name
        UNION ALL
        SELECT
            'HHT' AS act_type,
            '健康升级' AS act_type_name
        UNION ALL
        SELECT
            'HDX' AS act_type,
            '客户答谢' AS act_type_name
        UNION ALL
        SELECT
            'RSG' AS act_type,
            '健康到甲' AS act_type_name
        UNION ALL
        SELECT
            'ACTIVE0002' AS act_type,
            '平台活动' AS act_type_name
        UNION ALL
        SELECT
            'ZKW' AS act_type,
            '鹰瞳健康检测' AS act_type_name
        UNION ALL
        SELECT
            'IPN' AS act_type,
            '产品说明' AS act_type_name
        UNION ALL
        SELECT
            'TLE' AS act_type,
            '为爱加倍—权益领取会' AS act_type_name
        UNION ALL
        SELECT
            'ZSH' AS act_type,
            '太平惠汇战略合作洽谈会' AS act_type_name
        UNION ALL
        SELECT
            'XJHZCSH' AS act_type,
            '虓计划之创说会' AS act_type_name
        UNION ALL
        SELECT
            'ACTIVE0008' AS act_type,
            'fu计划之创说会' AS act_type_name
        UNION ALL
        SELECT
            'xkjwhd' AS act_type,
            '加温活动' AS act_type_name
        UNION ALL
        SELECT
            'ACTIVE0011' AS act_type,
            '长护险活动' AS act_type_name
        UNION ALL
        SELECT
            'ACTIVE0013' AS act_type,
            '家办活动' AS act_type_name
        UNION ALL
        SELECT
            'ACTIVE0009' AS act_type,
            '客服_美好权益荟' AS act_type_name
        UNION ALL
        SELECT
            'ACTIVE0010' AS act_type,
            '综拓活动' AS act_type_name
    ),
    data_max_date AS
    (
        SELECT max(customer_sign_time) AS data_time
        FROM dwd_wmup_llpt_sign_detail
    ),
    sign_base AS
    (
        SELECT
            if(isNotNull(t1.organ_id_1), t1.organ_id_1, '_') AS organ_id_1,
            if(isNotNull(t1.organ_name_1), t1.organ_name_1, '_') AS organ_name_1,
            if(isNotNull(t1.organ_id_2), t1.organ_id_2, '_') AS organ_id_2,
            if(isNotNull(t1.organ_name_2), t1.organ_name_2, '_') AS organ_name_2,
            if(isNotNull(t1.organ_id_3), t1.organ_id_3, '_') AS organ_id_3,
            if(isNotNull(t1.organ_name_3), t1.organ_name_3, '_') AS organ_name_3,
            if(isNotNull(t1.organ_id_4), t1.organ_id_4, '_') AS organ_id_4,
            if(isNotNull(t1.organ_name_4), t1.organ_name_4, '_') AS organ_name_4,
            if(isNotNull(t1.organ_id_5), t1.organ_id_5, '_') AS organ_id_5,
            if(isNotNull(t1.organ_name_5), t1.organ_name_5, '_') AS organ_name_5,
            t1.act_no AS act_no,
            t1.mobile AS mobile,
            t1.customer_sign_time AS customer_sign_time,
            t1.is_agent_mobile AS is_agent_mobile,
            t1.sign_id AS sign_id,
            t1.agent_code AS agent_code,
            t1.if_new_cust AS if_new_cust
        FROM dwd_wmup_llpt_sign_detail AS t1
        LEFT JOIN act_type_dict AS t2 ON lower(t1.activity_type) = lower(t2.act_type)
        WHERE (1 = 1) AND (is_agent_mobile = '否')
    ),
    convert_base AS
    (
        SELECT *
        FROM
        (
            SELECT
                t1.*,
                t2.act_type_name
            FROM dwd_wmup_llpt_sign_detail_with_convert AS t1
            LEFT JOIN act_type_dict AS t2 ON lower(t1.activity_type) = lower(t2.act_type)
        ) AS base
        WHERE (1 = 1) AND (is_agent_mobile = '否')
    ),
    new_cust_ind AS
    (
        SELECT
            mobile,
            max(if_new_cust) AS if_new_cust
        FROM dwd_wmup_llpt_sign_detail
        WHERE 1 = 1
        GROUP BY mobile
    ),
    sign_cnt_view AS
    (
        SELECT
            mobile,
            countDistinct(sign_id) AS sign_cnt,
            caseWithExpression(countDistinct(sign_id), 1, '1次', 2, '2次', 3, '3次', 4, '4次', 5, '5次', '5次以上') AS sign_cnt_desc
        FROM sign_base
        GROUP BY mobile
    ),
    sign_stats_view AS
    (
        SELECT
            sd.organ_id_1,
            sd.organ_name_1,
            NULL AS organ_id_2,
            NULL AS organ_name_2,
            NULL AS organ_id_3,
            NULL AS organ_name_3,
            NULL AS organ_id_4,
            NULL AS organ_name_4,
            NULL AS organ_id_5,
            NULL AS organ_name_5,
            sd.organ_id_1 AS organ_id,
            sd.organ_name_1 AS organ_name,
            1 AS organ_level,
            countDistinct(sd.act_no) AS act_cnt,
            countDistinct(sd.mobile) AS sign_cust_cnt,
            countDistinct(if(new_cust.if_new_cust = 1, sd.mobile, NULL)) AS sign_new_cust_cnt,
            countDistinct(if(new_cust.if_new_cust = 0, sd.mobile, NULL)) AS sign_old_cust_cnt,
            countDistinct(if(sd.is_agent_mobile = '否', concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS cust_sign_cnt,
            countDistinct(if((sd.is_agent_mobile = '否') AND (sd.if_new_cust = 1), concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS new_cust_sign_cnt,
            countDistinct(if((sd.is_agent_mobile = '否') AND (sd.if_new_cust = 0), concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS old_cust_sign_cnt,
            countDistinct(sd.agent_code) AS agent_cnt,
            countDistinct(if(sd.if_new_cust = 1, sd.agent_code, NULL)) AS nc_agent_cnt,
            countDistinct(if(sd.if_new_cust = 0, sd.agent_code, NULL)) AS oc_agent_cnt,
            countDistinct(if(sd.is_agent_mobile = '是', sd.agent_code, NULL)) AS sign_agt_cnt
        FROM sign_base AS sd
        LEFT JOIN sign_cnt_view AS scv ON sd.mobile = scv.mobile
        LEFT JOIN new_cust_ind AS new_cust ON sd.mobile = new_cust.mobile
        WHERE (1 = 1) AND isNotNull(organ_id_1)
        GROUP BY
            sd.organ_id_1,
            sd.organ_name_1
        UNION ALL
        SELECT
            sd.organ_id_1,
            sd.organ_name_1,
            sd.organ_id_2,
            sd.organ_name_2,
            NULL AS organ_id_3,
            NULL AS organ_name_3,
            NULL AS organ_id_4,
            NULL AS organ_name_4,
            NULL AS organ_id_5,
            NULL AS organ_name_5,
            sd.organ_id_2 AS organ_id,
            sd.organ_name_2 AS organ_name,
            2 AS organ_level,
            countDistinct(sd.act_no) AS act_cnt,
            countDistinct(sd.mobile) AS sign_cust_cnt,
            countDistinct(if(new_cust.if_new_cust = 1, sd.mobile, NULL)) AS sign_new_cust_cnt,
            countDistinct(if(new_cust.if_new_cust = 0, sd.mobile, NULL)) AS sign_old_cust_cnt,
            countDistinct(if(sd.is_agent_mobile = '否', concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS cust_sign_cnt,
            countDistinct(if((sd.is_agent_mobile = '否') AND (sd.if_new_cust = 1), concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS new_cust_sign_cnt,
            countDistinct(if((sd.is_agent_mobile = '否') AND (sd.if_new_cust = 0), concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS old_cust_sign_cnt,
            countDistinct(sd.agent_code) AS agent_cnt,
            countDistinct(if(sd.if_new_cust = 1, sd.agent_code, NULL)) AS nc_agent_cnt,
            countDistinct(if(sd.if_new_cust = 0, sd.agent_code, NULL)) AS oc_agent_cnt,
            countDistinct(if(sd.is_agent_mobile = '是', sd.agent_code, NULL)) AS sign_agt_cnt
        FROM sign_base AS sd
        LEFT JOIN sign_cnt_view AS scv ON sd.mobile = scv.mobile
        LEFT JOIN new_cust_ind AS new_cust ON sd.mobile = new_cust.mobile
        WHERE (1 = 1) AND isNotNull(organ_id_2)
        GROUP BY
            sd.organ_id_1,
            sd.organ_name_1,
            sd.organ_id_2,
            sd.organ_name_2
        UNION ALL
        SELECT
            sd.organ_id_1,
            sd.organ_name_1,
            sd.organ_id_2,
            sd.organ_name_2,
            sd.organ_id_3,
            sd.organ_name_3,
            NULL AS organ_id_4,
            NULL AS organ_name_4,
            NULL AS organ_id_5,
            NULL AS organ_name_5,
            sd.organ_id_3 AS organ_id,
            sd.organ_name_3 AS organ_name,
            3 AS organ_level,
            countDistinct(sd.act_no) AS act_cnt,
            countDistinct(sd.mobile) AS sign_cust_cnt,
            countDistinct(if(new_cust.if_new_cust = 1, sd.mobile, NULL)) AS sign_new_cust_cnt,
            countDistinct(if(new_cust.if_new_cust = 0, sd.mobile, NULL)) AS sign_old_cust_cnt,
            countDistinct(if(sd.is_agent_mobile = '否', concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS cust_sign_cnt,
            countDistinct(if((sd.is_agent_mobile = '否') AND (sd.if_new_cust = 1), concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS new_cust_sign_cnt,
            countDistinct(if((sd.is_agent_mobile = '否') AND (sd.if_new_cust = 0), concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS old_cust_sign_cnt,
            countDistinct(sd.agent_code) AS agent_cnt,
            countDistinct(if(sd.if_new_cust = 1, sd.agent_code, NULL)) AS nc_agent_cnt,
            countDistinct(if(sd.if_new_cust = 0, sd.agent_code, NULL)) AS oc_agent_cnt,
            countDistinct(if(sd.is_agent_mobile = '是', sd.agent_code, NULL)) AS sign_agt_cnt
        FROM sign_base AS sd
        LEFT JOIN sign_cnt_view AS scv ON sd.mobile = scv.mobile
        LEFT JOIN new_cust_ind AS new_cust ON sd.mobile = new_cust.mobile
        WHERE (1 = 1) AND isNotNull(organ_id_3)
        GROUP BY
            sd.organ_id_1,
            sd.organ_name_1,
            sd.organ_id_2,
            sd.organ_name_2,
            sd.organ_id_3,
            sd.organ_name_3
        UNION ALL
        SELECT
            sd.organ_id_1,
            sd.organ_name_1,
            sd.organ_id_2,
            sd.organ_name_2,
            sd.organ_id_3,
            sd.organ_name_3,
            sd.organ_id_4,
            sd.organ_name_4,
            NULL AS organ_id_5,
            NULL AS organ_name_5,
            sd.organ_id_4 AS organ_id,
            sd.organ_name_4 AS organ_name,
            4 AS organ_level,
            countDistinct(sd.act_no) AS act_cnt,
            countDistinct(sd.mobile) AS sign_cust_cnt,
            countDistinct(if(new_cust.if_new_cust = 1, sd.mobile, NULL)) AS sign_new_cust_cnt,
            countDistinct(if(new_cust.if_new_cust = 0, sd.mobile, NULL)) AS sign_old_cust_cnt,
            countDistinct(if(sd.is_agent_mobile = '否', concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS cust_sign_cnt,
            countDistinct(if((sd.is_agent_mobile = '否') AND (sd.if_new_cust = 1), concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS new_cust_sign_cnt,
            countDistinct(if((sd.is_agent_mobile = '否') AND (sd.if_new_cust = 0), concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS old_cust_sign_cnt,
            countDistinct(sd.agent_code) AS agent_cnt,
            countDistinct(if(sd.if_new_cust = 1, sd.agent_code, NULL)) AS nc_agent_cnt,
            countDistinct(if(sd.if_new_cust = 0, sd.agent_code, NULL)) AS oc_agent_cnt,
            countDistinct(if(sd.is_agent_mobile = '是', sd.agent_code, NULL)) AS sign_agt_cnt
        FROM sign_base AS sd
        LEFT JOIN sign_cnt_view AS scv ON sd.mobile = scv.mobile
        LEFT JOIN new_cust_ind AS new_cust ON sd.mobile = new_cust.mobile
        WHERE (1 = 1) AND isNotNull(organ_id_4)
        GROUP BY
            sd.organ_id_1,
            sd.organ_name_1,
            sd.organ_id_2,
            sd.organ_name_2,
            sd.organ_id_3,
            sd.organ_name_3,
            sd.organ_id_4,
            sd.organ_name_4
        UNION ALL
        SELECT
            sd.organ_id_1,
            sd.organ_name_1,
            sd.organ_id_2,
            sd.organ_name_2,
            sd.organ_id_3,
            sd.organ_name_3,
            sd.organ_id_4,
            sd.organ_name_4,
            sd.organ_id_5,
            sd.organ_name_5,
            sd.organ_id_5,
            sd.organ_name_5,
            5 AS organ_level,
            countDistinct(sd.act_no) AS act_cnt,
            countDistinct(sd.mobile) AS sign_cust_cnt,
            countDistinct(if(new_cust.if_new_cust = 1, sd.mobile, NULL)) AS sign_new_cust_cnt,
            countDistinct(if(new_cust.if_new_cust = 0, sd.mobile, NULL)) AS sign_old_cust_cnt,
            countDistinct(if(sd.is_agent_mobile = '否', concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS cust_sign_cnt,
            countDistinct(if((sd.is_agent_mobile = '否') AND (sd.if_new_cust = 1), concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS new_cust_sign_cnt,
            countDistinct(if((sd.is_agent_mobile = '否') AND (sd.if_new_cust = 0), concat(toString(sd.sign_id), toString(sd.mobile), toString(sd.customer_sign_time)), NULL)) AS old_cust_sign_cnt,
            countDistinct(sd.agent_code) AS agent_cnt,
            countDistinct(if(sd.if_new_cust = 1, sd.agent_code, NULL)) AS nc_agent_cnt,
            countDistinct(if(sd.if_new_cust = 0, sd.agent_code, NULL)) AS oc_agent_cnt,
            countDistinct(if(sd.is_agent_mobile = '是', sd.agent_code, NULL)) AS sign_agt_cnt
        FROM sign_base AS sd
        LEFT JOIN sign_cnt_view AS scv ON sd.mobile = scv.mobile
        LEFT JOIN new_cust_ind AS new_cust ON sd.mobile = new_cust.mobile
        WHERE (1 = 1) AND isNotNull(organ_id_5)
        GROUP BY
            sd.organ_id_1,
            sd.organ_name_1,
            sd.organ_id_2,
            sd.organ_name_2,
            sd.organ_id_3,
            sd.organ_name_3,
            sd.organ_id_4,
            sd.organ_name_4,
            sd.organ_id_5,
            sd.organ_name_5
    ),
    sign_pol_detail AS
    (
        SELECT
            sd.mobile AS mobile,
            scv.sign_cnt_desc AS sign_cnt_desc,
            '1' AS organ_id_1,
            '总公司' AS organ_name_1,
            sd.convert_organ_id_2 AS organ_id_2,
            sd.convert_organ_name_2 AS organ_name_2,
            sd.convert_organ_id_3 AS organ_id_3,
            sd.convert_organ_name_3 AS organ_name_3,
            sd.convert_organ_id_4 AS organ_id_4,
            sd.convert_organ_name_4 AS organ_name_4,
            sd.convert_organ_id_5 AS organ_id_5,
            sd.convert_organ_name_5 AS organ_name_5,
            sd.long_pol_flag AS long_pol_flag,
            sd.policy_code AS policy_code,
            sd.m_plc_period_prem AS plc_period_prem,
            sd.m_plc_prem_value AS plc_prem_value,
            sd.`30d_plc_period_prem` AS `30d_plc_period_prem`,
            sd.`30d_plc_prem_value` AS `30d_plc_prem_value`,
            sd.y_plc_period_prem AS y_plc_period_prem,
            sd.y_plc_prem_value AS y_plc_prem_value,
            sd.hold_customer_ind AS hold_customer_ind,
            sd.m_convert_ind AS m_convert_ind,
            sd.`30d_convert_ind` AS `30d_convert_ind`,
            sd.y_convert_ind AS y_convert_ind,
            new_cust.if_new_cust AS if_new_cust,
            row_number() OVER (PARTITION BY sd.policy_code, m_convert_ind ORDER BY sd.long_pol_flag DESC) AS m_pol_rn,
            row_number() OVER (PARTITION BY sd.policy_code, `30d_convert_ind` ORDER BY sd.long_pol_flag DESC) AS `30d_pol_rn`,
            row_number() OVER (PARTITION BY sd.policy_code, y_convert_ind ORDER BY sd.long_pol_flag DESC) AS y_pol_rn,
            row_number() OVER (PARTITION BY sd.policy_code, m_convert_ind, new_cust.if_new_cust ORDER BY sd.long_pol_flag DESC) AS new_m_pol_rn,
            row_number() OVER (PARTITION BY sd.policy_code, `30d_convert_ind`, new_cust.if_new_cust ORDER BY sd.long_pol_flag DESC) AS new_30d_pol_rn,
            row_number() OVER (PARTITION BY sd.policy_code, y_convert_ind, new_cust.if_new_cust ORDER BY sd.long_pol_flag DESC) AS new_y_pol_rn
        FROM convert_base AS sd
        LEFT JOIN sign_cnt_view AS scv ON sd.mobile = scv.mobile
        LEFT JOIN new_cust_ind AS new_cust ON sd.mobile = new_cust.mobile
        WHERE (1 = 1) AND isNotNull(sd.policy_code)
    ),
    sign_pol_detail_1 AS
    (
        SELECT
            mobile,
            sign_cnt_desc,
            organ_id_1,
            organ_name_1,
            organ_id_2,
            organ_name_2,
            organ_id_3,
            organ_name_3,
            organ_id_4,
            organ_name_4,
            organ_id_5,
            organ_name_5,
            long_pol_flag,
            policy_code,
            plc_period_prem,
            plc_prem_value,
            `30d_plc_period_prem`,
            `30d_plc_prem_value`,
            y_plc_period_prem,
            y_plc_prem_value,
            hold_customer_ind,
            m_convert_ind,
            `30d_convert_ind`,
            y_convert_ind,
            if_new_cust,
            m_pol_rn,
            `30d_pol_rn`,
            y_pol_rn,
            new_m_pol_rn,
            new_30d_pol_rn,
            new_y_pol_rn
        FROM sign_pol_detail
        GROUP BY
            mobile,
            sign_cnt_desc,
            organ_id_1,
            organ_name_1,
            organ_id_2,
            organ_name_2,
            organ_id_3,
            organ_name_3,
            organ_id_4,
            organ_name_4,
            organ_id_5,
            organ_name_5,
            long_pol_flag,
            policy_code,
            plc_period_prem,
            plc_prem_value,
            `30d_plc_period_prem`,
            `30d_plc_prem_value`,
            y_plc_period_prem,
            y_plc_prem_value,
            hold_customer_ind,
            m_convert_ind,
            `30d_convert_ind`,
            y_convert_ind,
            if_new_cust,
            m_pol_rn,
            `30d_pol_rn`,
            y_pol_rn,
            new_m_pol_rn,
            new_30d_pol_rn,
            new_y_pol_rn
    ),
    convert_stats_view AS
    (
        SELECT
            organ_id_1,
            organ_name_1,
            organ_id_2,
            organ_name_2,
            organ_id_3,
            organ_name_3,
            organ_id_4,
            organ_name_4,
            organ_id_5,
            organ_name_5,
            multiIf(isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNotNull(organ_id_3) AND isNotNull(organ_id_4) AND isNotNull(organ_id_5), organ_id_5, isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNotNull(organ_id_3) AND isNotNull(organ_id_4) AND isNull(organ_id_5), organ_id_4, isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNotNull(organ_id_3) AND isNull(organ_id_4), organ_id_3, isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNull(organ_id_3), organ_id_2, isNotNull(organ_id_1) AND isNull(organ_id_2), organ_id_1, NULL) AS organ_id,
            multiIf(isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNotNull(organ_id_3) AND isNotNull(organ_id_4) AND isNotNull(organ_id_5), organ_name_5, isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNotNull(organ_id_3) AND isNotNull(organ_id_4) AND isNull(organ_id_5), organ_name_4, isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNotNull(organ_id_3) AND isNull(organ_id_4), organ_name_3, isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNull(organ_id_3), organ_name_2, isNotNull(organ_id_1) AND isNull(organ_id_2), organ_name_1, NULL) AS organ_name,
            multiIf(isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNotNull(organ_id_3) AND isNotNull(organ_id_4) AND isNotNull(organ_id_5), 5, isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNotNull(organ_id_3) AND isNotNull(organ_id_4) AND isNull(organ_id_5), 4, isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNotNull(organ_id_3) AND isNull(organ_id_4), 3, isNotNull(organ_id_1) AND isNotNull(organ_id_2) AND isNull(organ_id_3), 2, isNotNull(organ_id_1) AND isNull(organ_id_2), 1, NULL) AS organ_level,
            countDistinct(if(m_convert_ind = 1, t1.mobile, NULL)) AS m_convert_custs,
            countDistinct(if(m_convert_ind = 1, policy_code, NULL)) AS m_pol_cnt,
            sum(if((m_pol_rn = 1) AND (m_convert_ind = 1), plc_period_prem, 0)) AS m_convert_prem,
            sum(if((m_pol_rn = 1) AND (m_convert_ind = 1), plc_prem_value, 0)) AS m_convert_value,
            countDistinct(if(`30d_convert_ind` = 1, t1.mobile, NULL)) AS `30d_convert_custs`,
            countDistinct(if(`30d_convert_ind` = 1, policy_code, NULL)) AS `30d_pol_cnt`,
            sum(if((`30d_pol_rn` = 1) AND (`30d_convert_ind` = 1), `30d_plc_period_prem`, 0)) AS `30d_convert_prem`,
            sum(if((`30d_pol_rn` = 1) AND (`30d_convert_ind` = 1), `30d_plc_prem_value`, 0)) AS `30d_convert_value`,
            countDistinct(if(y_convert_ind = 1, t1.mobile, NULL)) AS y_convert_custs,
            countDistinct(if(y_convert_ind = 1, policy_code, NULL)) AS y_pol_cnt,
            sum(if((y_pol_rn = 1) AND (y_convert_ind = 1), y_plc_period_prem, 0)) AS y_convert_prem,
            sum(if((y_pol_rn = 1) AND (y_convert_ind = 1), y_plc_prem_value, 0)) AS y_convert_value,
            countDistinct(if((m_convert_ind = 1) AND (if_new_cust = 1), t1.mobile, NULL)) AS new_m_convert_custs,
            countDistinct(if((m_convert_ind = 1) AND (if_new_cust = 1), policy_code, NULL)) AS new_m_pol_cnt,
            sum(if((new_m_pol_rn = 1) AND (m_convert_ind = 1) AND (if_new_cust = 1), plc_period_prem, 0)) AS new_m_convert_prem,
            sum(if((new_m_pol_rn = 1) AND (m_convert_ind = 1) AND (if_new_cust = 1), plc_prem_value, 0)) AS new_m_convert_value,
            countDistinct(if((`30d_convert_ind` = 1) AND (if_new_cust = 1), t1.mobile, NULL)) AS new_30d_convert_custs,
            countDistinct(if((`30d_convert_ind` = 1) AND (if_new_cust = 1), policy_code, NULL)) AS new_30d_pol_cnt,
            sum(if((new_30d_pol_rn = 1) AND (`30d_convert_ind` = 1) AND (if_new_cust = 1), `30d_plc_period_prem`, 0)) AS new_30d_convert_prem,
            sum(if((new_30d_pol_rn = 1) AND (`30d_convert_ind` = 1) AND (if_new_cust = 1), `30d_plc_prem_value`, 0)) AS new_30d_convert_value,
            countDistinct(if((y_convert_ind = 1) AND (if_new_cust = 1), t1.mobile, NULL)) AS new_y_convert_custs,
            countDistinct(if((y_convert_ind = 1) AND (if_new_cust = 1), policy_code, NULL)) AS new_y_pol_cnt,
            sum(if((new_y_pol_rn = 1) AND (y_convert_ind = 1) AND (if_new_cust = 1), y_plc_period_prem, 0)) AS new_y_convert_prem,
            sum(if((new_y_pol_rn = 1) AND (y_convert_ind = 1) AND (if_new_cust = 1), y_plc_prem_value, 0)) AS new_y_convert_value,
            countDistinct(if((m_convert_ind = 1) AND (if_new_cust = 0), t1.mobile, NULL)) AS old_m_convert_custs,
            countDistinct(if((m_convert_ind = 1) AND (if_new_cust = 0), policy_code, NULL)) AS old_m_pol_cnt,
            sum(if((new_m_pol_rn = 1) AND (m_convert_ind = 1) AND (if_new_cust = 0), plc_period_prem, 0)) AS old_m_convert_prem,
            sum(if((new_m_pol_rn = 1) AND (m_convert_ind = 1) AND (if_new_cust = 0), plc_prem_value, 0)) AS old_m_convert_value,
            countDistinct(if((`30d_convert_ind` = 1) AND (if_new_cust = 0), t1.mobile, NULL)) AS old_30d_convert_custs,
            countDistinct(if((`30d_convert_ind` = 1) AND (if_new_cust = 0), policy_code, NULL)) AS old_30d_pol_cnt,
            sum(if((new_30d_pol_rn = 1) AND (`30d_convert_ind` = 1) AND (if_new_cust = 0), `30d_plc_period_prem`, 0)) AS old_30d_convert_prem,
            sum(if((new_30d_pol_rn = 1) AND (`30d_convert_ind` = 1) AND (if_new_cust = 0), `30d_plc_prem_value`, 0)) AS old_30d_convert_value,
            countDistinct(if((y_convert_ind = 1) AND (if_new_cust = 0), t1.mobile, NULL)) AS old_y_convert_custs,
            countDistinct(if((y_convert_ind = 1) AND (if_new_cust = 0), policy_code, NULL)) AS old_y_pol_cnt,
            sum(if((new_y_pol_rn = 1) AND (y_convert_ind = 1) AND (if_new_cust = 0), y_plc_period_prem, 0)) AS old_y_convert_prem,
            sum(if((new_y_pol_rn = 1) AND (y_convert_ind = 1) AND (if_new_cust = 0), y_plc_prem_value, 0)) AS old_y_convert_value
        FROM sign_pol_detail_1 AS t1
        WHERE isNotNull(organ_id_1)
        GROUP BY
            GROUPING SETS (
             (organ_id_1, organ_name_1),
             (organ_id_1, organ_name_1, organ_id_2, organ_name_2),
             (organ_id_1, organ_name_1, organ_id_2, organ_name_2, organ_id_3, organ_name_3),
             (organ_id_1, organ_name_1, organ_id_2, organ_name_2, organ_id_3, organ_name_3, organ_id_4, organ_name_4),
             (organ_id_1, organ_name_1, organ_id_2, organ_name_2, organ_id_3, organ_name_3, organ_id_4, organ_name_4, organ_id_5, organ_name_5))
    )
SELECT
    t.organ_id_1 AS organ_id_1,
    t.organ_name_1 AS organ_name_1,
    t.organ_id_2 AS organ_id_2,
    t.organ_name_2 AS organ_name_2,
    t.organ_id_3 AS organ_id_3,
    t.organ_name_3 AS organ_name_3,
    t.organ_id_4 AS organ_id_4,
    t.organ_name_4 AS organ_name_4,
    t.organ_id_5 AS organ_id_5,
    t.organ_name_5 AS organ_name_5,
    t.organ_id AS organ_id,
    t.organ_name AS organ_name,
    t.organ_level AS organ_level,
    if(isNull(t1.act_cnt), 0, t1.act_cnt) AS act_cnt,
    if(isNull(t1.sign_cust_cnt), 0, t1.sign_cust_cnt) AS sign_cust_cnt,
    if(isNull(t1.sign_new_cust_cnt), 0, t1.sign_new_cust_cnt) AS sign_new_cust_cnt,
    if(isNull(t1.sign_old_cust_cnt), 0, t1.sign_old_cust_cnt) AS sign_old_cust_cnt,
    if(isNull(t1.cust_sign_cnt), 0, t1.cust_sign_cnt) AS cust_sign_cnt,
    if(isNull(t1.new_cust_sign_cnt), 0, t1.new_cust_sign_cnt) AS new_cust_sign_cnt,
    if(isNull(t1.old_cust_sign_cnt), 0, t1.old_cust_sign_cnt) AS old_cust_sign_cnt,
    if(isNull(t1.agent_cnt), 0, t1.agent_cnt) AS agent_cnt,
    if(isNull(t1.nc_agent_cnt), 0, t1.nc_agent_cnt) AS nc_agent_cnt,
    if(isNull(t1.oc_agent_cnt), 0, t1.oc_agent_cnt) AS oc_agent_cnt,
    if(isNull(t1.sign_agt_cnt), 0, t1.sign_agt_cnt) AS sign_agt_cnt,
    if(isNull(t2.m_convert_custs), 0, t2.m_convert_custs) AS m_convert_custs,
    if(isNull(t2.m_pol_cnt), 0, t2.m_pol_cnt) AS m_pol_cnt,
    if(isNull(t2.m_convert_prem), 0, t2.m_convert_prem) AS m_convert_prem,
    if(isNull(t2.m_convert_value), 0, t2.m_convert_value) AS m_convert_value,
    if(isNull(t2.`30d_convert_custs`), 0, t2.`30d_convert_custs`) AS `30d_convert_custs`,
    if(isNull(t2.`30d_pol_cnt`), 0, t2.`30d_pol_cnt`) AS `30d_pol_cnt`,
    if(isNull(t2.`30d_convert_prem`), 0, t2.`30d_convert_prem`) AS `30d_convert_prem`,
    if(isNull(t2.`30d_convert_value`), 0, t2.`30d_convert_value`) AS `30d_convert_value`,
    if(isNull(t2.y_convert_custs), 0, t2.y_convert_custs) AS y_convert_custs,
    if(isNull(t2.y_pol_cnt), 0, t2.y_pol_cnt) AS y_pol_cnt,
    if(isNull(t2.y_convert_prem), 0, t2.y_convert_prem) AS y_convert_prem,
    if(isNull(t2.y_convert_value), 0, t2.y_convert_value) AS y_convert_value,
    if(isNull(t2.new_m_convert_custs), 0, t2.new_m_convert_custs) AS new_m_convert_custs,
    if(isNull(t2.new_m_pol_cnt), 0, t2.new_m_pol_cnt) AS new_m_pol_cnt,
    if(isNull(t2.new_m_convert_prem), 0, t2.new_m_convert_prem) AS new_m_convert_prem,
    if(isNull(t2.new_m_convert_value), 0, t2.new_m_convert_value) AS new_m_convert_value,
    if(isNull(t2.new_30d_convert_custs), 0, t2.new_30d_convert_custs) AS new_30d_convert_custs,
    if(isNull(t2.new_30d_pol_cnt), 0, t2.new_30d_pol_cnt) AS new_30d_pol_cnt,
    if(isNull(t2.new_30d_convert_prem), 0, t2.new_30d_convert_prem) AS new_30d_convert_prem,
    if(isNull(t2.new_30d_convert_value), 0, t2.new_30d_convert_value) AS new_30d_convert_value,
    if(isNull(t2.new_y_convert_custs), 0, t2.new_y_convert_custs) AS new_y_convert_custs,
    if(isNull(t2.new_y_pol_cnt), 0, t2.new_y_pol_cnt) AS new_y_pol_cnt,
    if(isNull(t2.new_y_convert_prem), 0, t2.new_y_convert_prem) AS new_y_convert_prem,
    if(isNull(t2.new_y_convert_value), 0, t2.new_y_convert_value) AS new_y_convert_value,
    if(isNull(t2.old_m_convert_custs), 0, t2.old_m_convert_custs) AS old_m_convert_custs,
    if(isNull(t2.old_m_pol_cnt), 0, t2.old_m_pol_cnt) AS old_m_pol_cnt,
    if(isNull(t2.old_m_convert_prem), 0, t2.old_m_convert_prem) AS old_m_convert_prem,
    if(isNull(t2.old_m_convert_value), 0, t2.old_m_convert_value) AS old_m_convert_value,
    if(isNull(t2.old_30d_convert_custs), 0, t2.old_30d_convert_custs) AS old_30d_convert_custs,
    if(isNull(t2.old_30d_pol_cnt), 0, t2.old_30d_pol_cnt) AS old_30d_pol_cnt,
    if(isNull(t2.old_30d_convert_prem), 0, t2.old_30d_convert_prem) AS old_30d_convert_prem,
    if(isNull(t2.old_30d_convert_value), 0, t2.old_30d_convert_value) AS old_30d_convert_value,
    if(isNull(t2.old_y_convert_custs), 0, t2.old_y_convert_custs) AS old_y_convert_custs,
    if(isNull(t2.old_y_pol_cnt), 0, t2.old_y_pol_cnt) AS old_y_pol_cnt,
    if(isNull(t2.old_y_convert_prem), 0, t2.old_y_convert_prem) AS old_y_convert_prem,
    if(isNull(t2.old_y_convert_value), 0, t2.old_y_convert_value) AS old_y_convert_value,
    t3.data_time AS data_time
FROM dwd_wmup_llpt_organ_dict AS t
LEFT JOIN sign_stats_view AS t1 ON t.organ_id = t1.organ_id
LEFT JOIN convert_stats_view AS t2 ON t.organ_id = t2.organ_id
CROSS JOIN data_max_date AS t3;
