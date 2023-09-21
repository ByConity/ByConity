set enable_optimizer=1;
set dialect_type='ANSI';
set data_type_default_nullable=0;

DROP TABLE IF EXISTS idm_ods_xz_inst_xz_hic_app_user_evt_ri;
DROP TABLE IF EXISTS idm_ods_xz_inst_xz_hic_app_user_evt_ri;
DROP TABLE IF EXISTS idm_ods_xz_inst_xz_hic_web_user_evt_ri;
DROP TABLE IF EXISTS idm_ods_xz_inst_xz_hic_web_user_evt_ri;
DROP TABLE IF EXISTS idm_dwd_comm_xz_bh_page_mapping_df;
DROP TABLE IF EXISTS idm_dwd_comm_xz_bh_page_mapping_df;

CREATE TABLE idm_ods_xz_inst_xz_hic_app_user_evt_ri
(
    etltime DateTime ,
    recvtime DateTime ,
    time Nullable(DateTime) ,
    deviceid  Nullable(String) ,
    os Nullable(String),
    app_partner Nullable(String) ,
    brand Nullable(String) ,
    uniqueid Nullable(String),
    khh Nullable(String) ,
    id  Nullable(String) ,
    page_title Nullable(String) ,
    page_title_map Map(String,String) ,
    page_id Nullable(String) ,
    page_id_map  Map(String, String) ,
    btn_id  Nullable(String) ,
    btn_id_map  Map(String,String) ,
    btn_title Nullable(String) ,
    btn_title_map Map(String,String) ,
    param Nullable(String) ,
    param_map  Map(String,String) ,
    client_ip  Nullable(String),
    ip_country Nullable(String),
    ip_province  Nullable(String),
    ip_city Nullable(String),
    origin_map Map(String,String)
)
    ENGINE = CnchMergeTree() order by tuple();


CREATE TABLE idm_ods_xz_inst_xz_hic_web_user_evt_ri
(
    etltime DateTime ,
    product_id  Nullable(String) ,
    product_name Nullable(String) ,
    recvtime DateTime ,
    time Nullable(DateTime),
    device_id  Nullable(String) ,
    distinct_id Nullable(String) ,
    khh Nullable(String),
    user_name Nullable(String) ,
    browser Nullable(String) ,
    browser_version  Nullable(String) ,
    os Nullable(String),
    id  Nullable(String) ,
    page_id Nullable(String) ,
    page_id_map  Map(String, String) ,
    page_title  Nullable(String) ,
    page_title_map  Map(String,String),
    btn_id Nullable(String),
    btn_id_map  Map(String, String) ,
    btn_title Nullable(String),
    btn_title_map Map(String,String) ,
    url Nullable(String) ,
    previous Nullable(String) ,
    param Nullable(String) ,
    param_map Map(String,String),
    client_ip  Nullable(String) DEFAULT NULL,
    ip_country  Nullable(String) DEFAULT NULL,
    ip_province  Nullable(String) DEFAULT NULL,
    ip_city Nullable(String) DEFAULT NULL,
    product_type Nullable(String) ,
    channel_env Nullable(String) ,
    origin_map  Map(String,String)
)
    ENGINE = CnchMergeTree() order by tuple();

CREATE TABLE idm_dwd_comm_xz_bh_page_mapping_df
(
    channel Nullable(String),
    btn_id Nullable(String),
    page_id Nullable(String),
    page_title Nullable(String),
    resource_type Nullable(String),
    btn_title Nullable(String),
    entity_type Nullable(String),
    entity_cd Nullable(String),
    entity_sub_type Nullable(String),
    entity_sub_cd Nullable(String),
    bh_type Nullable(String),
    bh_cd Nullable(String),
    bh_sub_type Nullable(String),
    bh_sub_cd Nullable(String),
    distribute_key Nullable(String),
    p_date Date,
    business_type Nullable(String),
    is_split Nullable(String),
    has_permission Nullable(String)
)
    ENGINE = CnchMergeTree() order by tuple();

WITH tmp_page_mapping AS
         (
             SELECT
                 trimBoth(channel) AS channel,
                 trimBoth(btn_id) AS btn_id,
                 trimBoth(page_id) AS page_id,
                 trimBoth(page_title) AS page_title,
                 trimBoth(resource_type) AS resource_type,
                 trimBoth(btn_title) AS btn_title,
                 trimBoth(entity_type) AS entity_type,
                 trimBoth(entity_cd) AS entity_cd,
                 trimBoth(entity_sub_type) AS entity_sub_type,
                 trimBoth(entity_sub_cd) AS entity_sub_cd,
                 trimBoth(bh_type) AS bh_type,
                 trimBoth(bh_cd) AS bh_cd,
                 trimBoth(bh_sub_type) AS bh_sub_type,
                 trimBoth(bh_sub_cd) AS bh_sub_cd,
                 trimBoth(business_type) AS business_type,
                 trimBoth(is_split) AS is_split,
                 trimBoth(has_permission) AS has_permission,
                 trimBoth(distribute_key) AS distribute_key
             FROM idm_dwd_comm_xz_bh_page_mapping_df
             WHERE p_date IN (
                 SELECT max(p_date)
                 FROM idm_dwd_comm_xz_bh_page_mapping_df
             )
         ),
     tmp_app AS
         (
             SELECT
                 a.recv_time AS recv_time,
                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM
                 (
                     SELECT
                         a.recvtime AS recv_time,
                         a.page_title AS page_title,
                         a.page_id AS page_id,
                         a.btn_id AS btn_id,
                         a.btn_title AS btn_title,
                         a.page_title_map AS page_title_map,
                         a.page_id_map AS page_id_map,
                         a.btn_id_map AS btn_id_map,
                         a.btn_title_map AS btn_title_map,
                         a.time AS bh_time,
                         'APP' AS chnnl_cd
                     FROM idm_ods_xz_inst_xz_hic_app_user_evt_ri AS a
                     ) AS a
                     LEFT JOIN
                 (
                     SELECT *
                     FROM
                         (
                             SELECT
                                 *,
                                 row_number() OVER (PARTITION BY btn_id) AS rn
                             FROM tmp_page_mapping
                             WHERE channel = 'APP'
                             )
                     WHERE rn = 1
                     ) AS mapping_1 ON mapping_1.btn_id = a.btn_id
         ),
     temp_web_ri AS
         (
             SELECT
                 a.recvtime AS recv_time,
                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.time AS bh_time,
                 'WECHAT' AS chnnl_cd
             FROM idm_ods_xz_inst_xz_hic_web_user_evt_ri AS a
             WHERE a.recvtime >= toDate(now())
         ),
     temp_web_btn_id AS
         (
             SELECT
                 a.recv_time AS recv_time,
                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_ri AS a
                      LEFT JOIN
                  (
                      SELECT *
                      FROM
                          (
                              SELECT
                                  *,
                                  row_number() OVER (PARTITION BY btn_id) AS rn
                              FROM tmp_page_mapping
                              WHERE channel = 'APP'
                              ) WHERE rn = 1
                      ) AS mapping_1 ON (mapping_1.channel = a.chnnl_cd) AND (mapping_1.btn_id = a.btn_id)
         ),
     temp_web_page_title AS
         (
             SELECT
                 a.recv_time AS recv_time,
                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_ri AS a
                      LEFT JOIN
                  (
                      SELECT *
                      FROM
                          (
                              SELECT
                                  *,
                                  row_number() OVER (PARTITION BY btn_id) AS rn
                              FROM tmp_page_mapping
                              WHERE channel = 'APP'
                              ) WHERE rn = 1
                      ) AS mapping_1 ON (mapping_1.channel = a.chnnl_cd) AND (mapping_1.page_title = a.page_title)
         ),
     temp_web_btn_title AS
         (
             SELECT
                 a.recv_time AS recv_time,
                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_ri AS a
                      LEFT JOIN
                  (
                      SELECT *
                      FROM
                          (
                              SELECT
                                  *,
                                  row_number() OVER (PARTITION BY btn_id) AS rn
                              FROM tmp_page_mapping
                              WHERE channel = 'APP'
                              ) WHERE rn = 1
                      ) AS mapping_1 ON (mapping_1.channel = a.chnnl_cd) AND (mapping_1.btn_title = a.btn_title)
         ),
     temp_web AS
         (
             SELECT
                 a.recv_time AS recv_time,
                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_btn_id AS a
             UNION ALL
             SELECT
                 a.recv_time AS recv_time,
                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_page_title AS a
             UNION ALL
             SELECT
                 a.recv_time AS recv_time,
                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_btn_title AS a
         )
SELECT DISTINCT
    a.recv_time AS recv_time,
    a.page_title AS page_title,
    a.page_id AS page_id,
    a.btn_id AS btn_id,
    a.btn_title AS btn_title,
    a.page_title_map AS page_title_map,
    a.page_id_map AS page_id_map,
    a.btn_id_map AS btn_id_map,
    a.btn_title_map AS btn_title_map,
    a.bh_time,
    a.chnnl_cd
FROM tmp_app AS a
UNION ALL
WITH tmp_page_mapping AS
         (
             SELECT
                 trimBoth(channel) AS channel,
                 trimBoth(btn_id) AS btn_id,
                 trimBoth(page_id) AS page_id,
                 trimBoth(page_title) AS page_title,
                 trimBoth(resource_type) AS resource_type,
                 trimBoth(btn_title) AS btn_title,
                 trimBoth(entity_type) AS entity_type,
                 trimBoth(entity_cd) AS entity_cd,
                 trimBoth(entity_sub_type) AS entity_sub_type,
                 trimBoth(entity_sub_cd) AS entity_sub_cd,
                 trimBoth(bh_type) AS bh_type,
                 trimBoth(bh_cd) AS bh_cd,
                 trimBoth(bh_sub_type) AS bh_sub_type,
                 trimBoth(bh_sub_cd) AS bh_sub_cd,
                 trimBoth(business_type) AS business_type,
                 trimBoth(is_split) AS is_split,
                 trimBoth(has_permission) AS has_permission,
                 trimBoth(distribute_key) AS distribute_key
             FROM idm_dwd_comm_xz_bh_page_mapping_df
             WHERE p_date IN (
                 SELECT max(p_date)
                 FROM idm_dwd_comm_xz_bh_page_mapping_df
             )
         ),
     tmp_app AS
         (
             SELECT
                 a.recv_time AS recv_time,
                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM
                 (
                     SELECT
                         a.recvtime AS recv_time,
                         a.page_title AS page_title,
                         a.page_id AS page_id,
                         a.btn_id AS btn_id,
                         a.btn_title AS btn_title,
                         a.page_title_map AS page_title_map,
                         a.page_id_map AS page_id_map,
                         a.btn_id_map AS btn_id_map,
                         a.btn_title_map AS btn_title_map,
                         a.time AS bh_time,
                         'APP' AS chnnl_cd
                     FROM idm_ods_xz_inst_xz_hic_app_user_evt_ri AS a
                     ) AS a
                     LEFT JOIN
                 (
                     SELECT *
                     FROM
                         (
                             SELECT
                                 *,
                                 row_number() OVER (PARTITION BY btn_id) AS rn
                             FROM tmp_page_mapping
                             WHERE channel = 'APP'
                             )
                     WHERE rn = 1
                     ) AS mapping_1 ON mapping_1.btn_id = a.btn_id
         ),
     temp_web_ri AS
         (
             SELECT
                 a.recvtime AS recv_time,
                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.time AS bh_time,
                 'WECHAT' AS chnnl_cd
             FROM idm_ods_xz_inst_xz_hic_web_user_evt_ri AS a
             WHERE a.recvtime >= toDate(now())
         ),
     temp_web_btn_id AS
         (
             SELECT
                 a.recv_time AS recv_time,
                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_ri AS a
                      LEFT JOIN
                  (
                      SELECT *
                      FROM
                          (
                              SELECT
                                  *,
                                  row_number() OVER (PARTITION BY btn_id) AS rn
                              FROM tmp_page_mapping
                              WHERE channel = 'APP'
                              ) WHERE rn = 1
                      ) AS mapping_1 ON (mapping_1.channel = a.chnnl_cd) AND (mapping_1.btn_id = a.btn_id)
         ),
     temp_web_page_title AS
         (
             SELECT
                 a.recv_time AS recv_time,

                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_ri AS a
                      LEFT JOIN
                  (
                      SELECT *
                      FROM
                          (
                              SELECT
                                  *,
                                  row_number() OVER (PARTITION BY btn_id) AS rn
                              FROM tmp_page_mapping
                              WHERE channel = 'APP'
                              ) WHERE rn = 1
                      ) AS mapping_1 ON (mapping_1.channel = a.chnnl_cd) AND (mapping_1.page_title = a.page_title)
         ),
     temp_web_btn_title AS
         (
             SELECT
                 a.recv_time AS recv_time,

                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_ri AS a
                      LEFT JOIN
                  (
                      SELECT *
                      FROM
                          (
                              SELECT
                                  *,
                                  row_number() OVER (PARTITION BY btn_id) AS rn
                              FROM tmp_page_mapping
                              WHERE channel = 'APP'
                              ) WHERE rn = 1
                      ) AS mapping_1 ON (mapping_1.channel = a.chnnl_cd) AND (mapping_1.btn_title = a.btn_title)
         ),
     temp_web AS
         (
             SELECT
                 a.recv_time AS recv_time,

                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_btn_id AS a
             UNION ALL
             SELECT
                 a.recv_time AS recv_time,

                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_page_title AS a
             UNION ALL
             SELECT
                 a.recv_time AS recv_time,

                 a.page_title AS page_title,
                 a.page_id AS page_id,
                 a.btn_id AS btn_id,
                 a.btn_title AS btn_title,
                 a.page_title_map AS page_title_map,
                 a.page_id_map AS page_id_map,
                 a.btn_id_map AS btn_id_map,
                 a.btn_title_map AS btn_title_map,
                 a.bh_time,
                 a.chnnl_cd
             FROM temp_web_btn_title AS a
         )
SELECT DISTINCT
    a.recv_time AS recv_time,
    a.page_title AS page_title,
    a.page_id AS page_id,
    a.btn_id AS btn_id,
    a.btn_title AS btn_title,
    a.page_title_map AS page_title_map,
    a.page_id_map AS page_id_map,
    a.btn_id_map AS btn_id_map,
    a.btn_title_map AS btn_title_map,
    a.bh_time,
    a.chnnl_cd
FROM temp_web AS a;
