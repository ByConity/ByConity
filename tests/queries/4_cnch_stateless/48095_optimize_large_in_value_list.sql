CREATE DATABASE IF NOT EXISTS 48095_test;

USE 48095_test;

DROP TABLE IF EXISTS 48095_test.dws_ad_logs_rt_not_distinct_local;
DROP TABLE IF EXISTS 48095_test.dws_ad_logs_rt_not_distinct;

DROP TABLE IF EXISTS 48095_test.platform_ad_plan_local;
DROP TABLE IF EXISTS 48095_test.platform_ad_plan;

CREATE TABLE dws_ad_logs_rt_not_distinct
(
    `kind` LowCardinality(String),
    `id` String,
    `request_id` String NOT NULL,
    `bid` Nullable(String),
    `app_version_name` LowCardinality(Nullable(String)),
    `sdk_version` LowCardinality(Nullable(String)),
    `channel_package` LowCardinality(Nullable(String)),
    `sex` LowCardinality(Nullable(String)),
    `age_range` LowCardinality(Nullable(String)),
    `area_province` LowCardinality(Nullable(String)),
    `device_brand` LowCardinality(Nullable(String)),
    `device_range_sys_version` LowCardinality(Nullable(String)),
    `internal_install` LowCardinality(Nullable(String)),
    `network_state` LowCardinality(Nullable(String)),
    `unit_id` LowCardinality(String),
    `dsp_id` LowCardinality(Nullable(String)),
    `type` LowCardinality(Nullable(String)),
    `pay_type` LowCardinality(Nullable(String)),
    `success_code` LowCardinality(Nullable(String)),
    `ad_plan_id` Nullable(String),
    `ad_group_id` Nullable(String),
    `uid` String,
    `ad_cost` Nullable(Float64),
    `ad_gsp_cost` Nullable(Float64),
    `is_supplyment` LowCardinality(Nullable(String)),
    `recv_timestamp` String,
    `insert_timestamp` String,
    `ocpm_bid` Nullable(String),
    `ymd` Date
)
ENGINE = CnchMergeTree() order by id;


CREATE TABLE platform_ad_plan
(
    `apid` Int32 NOT NULL,
    `packagename` Nullable(String),
    `rec_game_flag` Nullable(String)
)
ENGINE = CnchMergeTree() order by apid;

set enable_optimizer=1;

EXPLAIN stats = 0
SELECT
   count(*)
FROM
(
    SELECT *
    FROM dws_ad_logs_rt_not_distinct
    WHERE (ymd = toDate('2024-06-27 00:00:00')) AND (recv_timestamp >= '2024-06-27 00:00:00') AND (recv_timestamp <= '2024-06-27 23:59:59')
) AS t1
LEFT JOIN platform_ad_plan AS t2 ON t1.ad_plan_id = CAST(t2.apid, 'text')
WHERE (ad_plan_id IN ('131078', '131061', '89270', '154810', '89272', '154811', '89275', '154808', '89274')) AND (uid IN ('67921ae675474d49b96f982ea6936363', '534757bd6e5340309cc119e54f23d4d7', '4f640b0110e647668df11478573dab9b')) AND (dsp_id IN ('1000001')) AND (kind IN ('', '')) settings max_in_value_list_to_pushdown= 100;

EXPLAIN stats = 0
SELECT
    count(*)
FROM
(
    SELECT *
    FROM dws_ad_logs_rt_not_distinct
    WHERE (ymd = toDate('2024-06-27 00:00:00')) AND (recv_timestamp >= '2024-06-27 00:00:00') AND (recv_timestamp <= '2024-06-27 23:59:59')
) AS t1
LEFT JOIN platform_ad_plan AS t2 ON t1.ad_plan_id = CAST(t2.apid, 'text')
WHERE (ad_plan_id GLOBAL IN ('131078', '131061', '89270', '154810', '89272', '154811', '89275', '154808', '89274')) AND (uid IN ('67921ae675474d49b96f982ea6936363', '534757bd6e5340309cc119e54f23d4d7', '4f640b0110e647668df11478573dab9b')) AND (dsp_id IN ('1000001')) AND (kind IN ('', '')) settings max_in_value_list_to_pushdown= 100;

EXPLAIN stats = 0
SELECT
    count(*)
FROM
(
    SELECT *
    FROM dws_ad_logs_rt_not_distinct
    WHERE (ymd = toDate('2024-06-27 00:00:00')) AND (recv_timestamp >= '2024-06-27 00:00:00') AND (recv_timestamp <= '2024-06-27 23:59:59')
) AS t1
LEFT JOIN platform_ad_plan AS t2 ON t1.ad_plan_id = CAST(t2.apid, 'text')
WHERE (ad_plan_id NOT IN ('131078', '131061', '89270', '154810', '89272', '154811', '89275', '154808', '89274')) AND (uid IN ('67921ae675474d49b96f982ea6936363', '534757bd6e5340309cc119e54f23d4d7', '4f640b0110e647668df11478573dab9b')) AND (dsp_id IN ('1000001')) AND (kind IN ('', '')) settings max_in_value_list_to_pushdown= 100;

EXPLAIN stats = 0
SELECT
    count(*)
FROM
(
    SELECT *
    FROM dws_ad_logs_rt_not_distinct
    WHERE (ymd = toDate('2024-06-27 00:00:00')) AND (recv_timestamp >= '2024-06-27 00:00:00') AND (recv_timestamp <= '2024-06-27 23:59:59')
) AS t1
LEFT JOIN platform_ad_plan AS t2 ON t1.ad_plan_id = CAST(t2.apid, 'text')
WHERE (ad_plan_id GLOBAL NOT IN ('131078', '131061', '89270', '154810', '89272', '154811', '89275', '154808', '89274')) AND (uid IN ('67921ae675474d49b96f982ea6936363', '534757bd6e5340309cc119e54f23d4d7', '4f640b0110e647668df11478573dab9b')) AND (dsp_id IN ('1000001')) AND (kind IN ('', '')) settings max_in_value_list_to_pushdown= 100;

-- set max_in_value_list_to_pushdown = 5, 
-- ad_plan_id IN ('131078', '131061', '89270', '154810', '89272', '154811', '89275', '154808', '89274') will not be pushdown.
EXPLAIN stats = 0
SELECT
    count(*)
FROM
(
    SELECT *
    FROM dws_ad_logs_rt_not_distinct
    WHERE (ymd = toDate('2024-06-27 00:00:00')) AND (recv_timestamp >= '2024-06-27 00:00:00') AND (recv_timestamp <= '2024-06-27 23:59:59')
) AS t1
LEFT JOIN platform_ad_plan AS t2 ON t1.ad_plan_id = CAST(t2.apid, 'text')
WHERE (ad_plan_id IN ('131078', '131061', '89270', '154810', '89272', '154811', '89275', '154808', '89274')) AND (uid IN ('67921ae675474d49b96f982ea6936363', '534757bd6e5340309cc119e54f23d4d7', '4f640b0110e647668df11478573dab9b')) AND (dsp_id IN ('1000001')) AND (kind IN ('', '')) settings max_in_value_list_to_pushdown= 5;

EXPLAIN stats = 0
SELECT
    count(*)
FROM
(
    SELECT *
    FROM dws_ad_logs_rt_not_distinct
    WHERE (ymd = toDate('2024-06-27 00:00:00')) AND (recv_timestamp >= '2024-06-27 00:00:00') AND (recv_timestamp <= '2024-06-27 23:59:59')
) AS t1
LEFT JOIN platform_ad_plan AS t2 ON t1.ad_plan_id = CAST(t2.apid, 'text')
WHERE (ad_plan_id GLOBAL IN ('131078', '131061', '89270', '154810', '89272', '154811', '89275', '154808', '89274')) AND (uid IN ('67921ae675474d49b96f982ea6936363', '534757bd6e5340309cc119e54f23d4d7', '4f640b0110e647668df11478573dab9b')) AND (dsp_id IN ('1000001')) AND (kind IN ('', '')) settings max_in_value_list_to_pushdown= 5;

EXPLAIN stats = 0
SELECT
    count(*)
FROM
(
    SELECT *
    FROM dws_ad_logs_rt_not_distinct
    WHERE (ymd = toDate('2024-06-27 00:00:00')) AND (recv_timestamp >= '2024-06-27 00:00:00') AND (recv_timestamp <= '2024-06-27 23:59:59')
) AS t1
LEFT JOIN platform_ad_plan AS t2 ON t1.ad_plan_id = CAST(t2.apid, 'text')
WHERE (ad_plan_id NOT IN ('131078', '131061', '89270', '154810', '89272', '154811', '89275', '154808', '89274')) AND (uid IN ('67921ae675474d49b96f982ea6936363', '534757bd6e5340309cc119e54f23d4d7', '4f640b0110e647668df11478573dab9b')) AND (dsp_id IN ('1000001')) AND (kind IN ('', '')) settings max_in_value_list_to_pushdown= 5;

EXPLAIN stats = 0
SELECT
    count(*)
FROM
(
    SELECT *
    FROM dws_ad_logs_rt_not_distinct
    WHERE (ymd = toDate('2024-06-27 00:00:00')) AND (recv_timestamp >= '2024-06-27 00:00:00') AND (recv_timestamp <= '2024-06-27 23:59:59')
) AS t1
LEFT JOIN platform_ad_plan AS t2 ON t1.ad_plan_id = CAST(t2.apid, 'text')
WHERE (ad_plan_id GLOBAL NOT IN ('131078', '131061', '89270', '154810', '89272', '154811', '89275', '154808', '89274')) AND (uid IN ('67921ae675474d49b96f982ea6936363', '534757bd6e5340309cc119e54f23d4d7', '4f640b0110e647668df11478573dab9b')) AND (dsp_id IN ('1000001')) AND (kind IN ('', '')) settings max_in_value_list_to_pushdown= 5;

DROP DATABASE IF EXISTS 48095_test;