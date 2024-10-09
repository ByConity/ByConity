DROP TABLE IF EXISTS idm_ods_ods_inst_user_tags;
DROP TABLE IF EXISTS idm_ana_xz_rpt_user_evt;
 CREATE TABLE idm_ods_ods_inst_user_tags
(
    `id` Int64 COMMENT '表id',
    `user_id` Nullable(Int64) COMMENT '用户id',
    `tag_type` Nullable(String) COMMENT '标签类型',
    `tag_value` Nullable(String) COMMENT '标签值',
    `parent_tag_id` Nullable(String) COMMENT '父标签id',
    `created_at` Nullable(DateTime) COMMENT '创建时间',
    `updated_at` Nullable(DateTime) COMMENT '更新时间',
    `created_by` Nullable(String) COMMENT '创建人',
    `updated_by` Nullable(String) COMMENT '更新人',
    `hd_business_data` Nullable(String) COMMENT '业务日期',
    `unique_id` Nullable(String) COMMENT '设备id',
    `hdfs_par` Nullable(String)
)
ENGINE = CnchMergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 8192;

CREATE TABLE idm_ana_xz_rpt_user_evt
(
    `device_id` String,
    `os` Nullable(String),
    `session_id` Nullable(String),
    `user_id` Nullable(Int64),
    `user_type` Nullable(String),
    `ip` Nullable(String),
    `city` Nullable(String),
    `prov` Nullable(String),
    `cntry` Nullable(String),
    `brand` Nullable(String),
    `event_type` Nullable(String),
    `page_title` Nullable(String),
    `page id` Nullable(String),
    `btn_id` String,
    `btn_title` Nullable(String),
    `param` Nullable(String),
    `recive_time` Nullable(DateTime),
    `entity_type` String,
    `entity_cd` String,
    `entity sub_type` String,
    `entity_sub_cd` String,
    `bh_type` String,
    `bh_cd` String,
    `bh_sub_type` String,
    `bh_sub_cd` String,
    `bh_time` Nullable(DateTime),
    `channel` Nullable(String),
    `specail param` Nullable(String),
    `p_date` Date,
    `model` Nullable(String),
    `has_permission` Nullable(Int32),
    `cntt_id` Nullable(String) COMMENT '内容id',
    `model_nm` Nullable(String) COMMENT '模块名称',
    `sharer_user_id` Nullable(Int64) COMMENT '分享人的用户id'
)
ENGINE = CnchMergeTree()
ORDER BY (p_date, btn_id, entity_cd);

set enable_optimizer=1;

EXPLAIN
WITH user_tags AS
    (
        SELECT
            user_id,
            p_date
        FROM
        (
            SELECT
                user_id AS user_id,
                toDate(hdfs_par) AS p_date
            FROM idm_ods_ods_inst_user_tags AS a
            WHERE p_date >= '2020-04-07'
        ) AS user_tags_original
        GROUP BY
            user_id,
            p_date
    )

SELECT count(1)
FROM
(
    SELECT
        c.*,
        e.*
    FROM
    (
        SELECT *
        FROM idm_ana_xz_rpt_user_evt
        WHERE (p_date >= '2020-04-07') AND (channel = 'APP')
    ) AS c
    LEFT JOIN user_tags AS e ON (c.user_id = e.user_id) AND (c.p_date = e.p_date)
    LEFT JOIN user_tags AS f ON c.p_date = f.p_date
) AS a;
