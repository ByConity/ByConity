DROP TABLE IF EXISTS adongxcxmdext_marketing_account_finance_daily_data ;
DROP TABLE IF EXISTS adongxcxmdext_agent_account_rebate ;
DROP TABLE IF EXISTS adongxcxmdext_agent ;
DROP TABLE IF EXISTS adongxcxmdext_agent_account ;
DROP TABLE IF EXISTS adongxcxmdext_channel ;
DROP TABLE IF EXISTS central_qgm_app ;
DROP TABLE IF EXISTS central_qgm_app_group ;
DROP TABLE IF EXISTS central_agent;
DROP TABLE IF EXISTS central_agent_account;
DROP TABLE IF EXISTS central_channel;
DROP TABLE IF EXISTS midukyy_full_att_adid_active_daily_report_v1;
DROP TABLE IF EXISTS midukyy_agent_account_rebate;
DROP TABLE IF EXISTS midukyy_agent;
DROP TABLE IF EXISTS midukyy_agent_account;
DROP TABLE IF EXISTS midukyy_channel;
DROP TABLE IF EXISTS midukyy_full_att_realtime_std_daily_report_v1;
DROP TABLE IF EXISTS midukyy_marketing_account_finance_daily_data;
DROP TABLE IF EXISTS midukyy_marketing_daily_data_v2;
DROP TABLE IF EXISTS central_agent_account;
DROP TABLE IF EXISTS central_channel;

CREATE TABLE adongxcxmdext_marketing_account_finance_daily_data
(
    `id` Int64,
    `date` Date,
    `agent_account_id` Int32 ,
    `consumption` Decimal(10, 2) DEFAULT '0.00' ,
    `consumption_real` Decimal(10, 2) DEFAULT '0.00' ,
    `cash_consumption` Decimal(10, 2) DEFAULT '0.00' ,
    `cash_consumption_real` Decimal(10, 2) DEFAULT '0.00' ,
    `grant_consumption` Decimal(10, 2) DEFAULT '0.00' ,
    `grant_consumption_real` Decimal(10, 2) DEFAULT '0.00' ,
    `payback` Decimal(10, 2) DEFAULT '0.00' ,
    `create_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `update_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `contract_rebate_consumption` Decimal(10, 2) DEFAULT '0.00' ,
    `contract_rebate_consumption_real` Decimal(10, 2) DEFAULT '0.00'
)
ENGINE = CnchMergeTree
PARTITION BY toYYYYMM(date)
CLUSTER BY (id, date) INTO 2 BUCKETS
ORDER BY (id, date)
UNIQUE KEY (id, date)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE adongxcxmdext_agent_account_rebate
(
    `id` Int64,
    `agent_account_id` Nullable(Int32) DEFAULT '0',
    `agent_id` Nullable(Int32) DEFAULT '0' ,
    `agent_name` Nullable(String) DEFAULT '' ,
    `opt_agent_id` Nullable(Int32) DEFAULT '0' ,
    `opt_agent_name` Nullable(String) DEFAULT '' ,
    `cur_agent_id` Int64 DEFAULT '0' ,
    `cur_agent_name` Nullable(String) DEFAULT '' ,
    `agent_type` Int16 DEFAULT '0' ,
    `date` Nullable(Date),
    `rate` Nullable(Decimal(10, 2)) DEFAULT '0.00',
    `fees` Nullable(Decimal(10, 2)),
    `update_time` Nullable(DateTime64(3)) DEFAULT now64() ,
    `create_time` Nullable(DateTime64(3)) DEFAULT now64()
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 2 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';


CREATE TABLE adongxcxmdext_agent
(
    `id` Int64,
    `name` String ,
    `channel_category_ids` String ,
    `channel_ids` String ,
    `company_name` String ,
    `memo` String ,
    `admin_id` Int64 DEFAULT '0' ,
    `admin_name` String ,
    `status` Int8 DEFAULT '0' ,
    `create_time` Nullable(DateTime64(3)) ,
    `update_time` Nullable(DateTime64(3)) DEFAULT now64() ,
    `sort` Int32 DEFAULT '0' ,
    `contact` String DEFAULT '' ,
    `platform_agent_id` String DEFAULT '' ,
    `agent_group_id` Nullable(Int64) DEFAULT '0'
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 2 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';


CREATE TABLE adongxcxmdext_agent_account
(
    `id` Int64,
    `channel_category_id` Int64 DEFAULT '0' ,
    `channel_id` Int32 DEFAULT '0' ,
    `agent_id` Nullable(Int32) DEFAULT '0' ,
    `agent_name` Nullable(String) DEFAULT '' ,
    `opt_agent_id` Nullable(Int32) DEFAULT '0' ,
    `opt_agent_name` Nullable(String) DEFAULT '' ,
    `username` String ,
    `password` String ,
    `os` Nullable(Int16) DEFAULT '0' ,
    `is_report_active` Int16 DEFAULT '0' ,
    `active_params` String ,
    `rebate_config` String ,
    `wx_mp_name` String ,
    `admin_id` Int64 DEFAULT '0' ,
    `admin_name` String ,
    `status` Int8 DEFAULT '0' ,
    `brand_logo` Nullable(String) ,
    `brand_logo_id` Nullable(String) DEFAULT '' ,
    `is_created_in_ug` Nullable(Int64) DEFAULT '0' ,
    `complete_progress` Nullable(Int64) DEFAULT '0' ,
    `deep_callback_behavior` Nullable(String) DEFAULT '' ,
    `callback_behavior` Nullable(String) DEFAULT '' ,
    `callback_type` Nullable(String) DEFAULT '' ,
    `hw_oaid_ip_key` Nullable(String) DEFAULT '' ,
    `hw_sign` Nullable(String) DEFAULT '' ,
    `akey` Nullable(String) DEFAULT '' ,
    `gdt_ios_data_src_id` Nullable(String) DEFAULT '' ,
    `gdt_android_data_src_id` Nullable(String) DEFAULT '' ,
    `wx_merchant_id` Nullable(String) DEFAULT '' ,
    `wx_ios_id` Nullable(String) DEFAULT '' ,
    `wx_android_id` Nullable(String) DEFAULT '' ,
    `wx_app_key` Nullable(String) DEFAULT '' ,
    `wx_app_id` Nullable(String) DEFAULT '' ,
    `wx_advertiser_id` Nullable(String) DEFAULT '' ,
    `ios_monitor_url` Nullable(String) ,
    `android_monitor_url` Nullable(String) ,
    `api_secret` Nullable(String) ,
    `api_uuid` Nullable(String) ,
    `data_source_id` Nullable(String) ,
    `dsp_grant_status` Nullable(Int16) DEFAULT '0' ,
    `dsp_grant_url` Nullable(String) ,
    `exchange_flow_app_id` Nullable(Int64) DEFAULT '0' ,
    `competition_account_id` Nullable(String) DEFAULT '' ,
    `competition_id` Nullable(String) DEFAULT '' ,
    `activity_name` Nullable(String) DEFAULT '' ,
    `resource_location` Nullable(String) DEFAULT '' ,
    `account_tag` Nullable(String) DEFAULT '' ,
    `growth_type` Nullable(Int16) DEFAULT '1' ,
    `step_complete_status` Nullable(String) ,
    `extra` Nullable(String) ,
    `image_token` Nullable(String) ,
    `intelligent_package_name` Nullable(String) ,
    `user_action_id` Nullable(String) ,
    `promoted_object_id` Nullable(String) ,
    `dsp_avatar_url` Nullable(String) ,
    `dsp_avatar_id` Nullable(String) DEFAULT '' ,
    `account_type` Int8 DEFAULT '1' ,
    `put_type` Int8 DEFAULT '0' ,
    `platform_account_id` Nullable(String) DEFAULT '' ,
    `account_company` Nullable(String) DEFAULT '' ,
    `account_pay_company` Nullable(String) DEFAULT '' ,
    `app_id` Int64 DEFAULT '0' ,
    `create_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `update_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `create_name` Nullable(String) DEFAULT '' ,
    `create_id` Nullable(Decimal(20, 0)) DEFAULT '0' ,
    `bid_creative_id` String DEFAULT '' ,
    `modifier_id` Nullable(Decimal(20, 0)) DEFAULT '0' ,
    `modifier_name` Nullable(String) DEFAULT '' ,
    `client_id` Nullable(String) DEFAULT '' ,
    `secret_key` Nullable(String) DEFAULT '' ,
    `is_outer` Int8 DEFAULT '0' ,
    `android_monitor_url_v2` Nullable(String) ,
    `mkt_api_version` Nullable(String) DEFAULT '' ,
    `old_user_switch` Nullable(Int8) DEFAULT '0' ,
    `active_callback_behavior` Nullable(String) DEFAULT '' ,
    `abtest_layer` Nullable(String) DEFAULT ''
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 2 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE adongxcxmdext_channel
(
    `id` Int64,
    `channel_category_id` Int64 DEFAULT '0' ,
    `name` String ,
    `url` String ,
    `short_name` String ,
    `admin_id` Decimal(20, 0) DEFAULT '0' ,
    `admin_name` String ,
    `status` Int8 DEFAULT '0' ,
    `spider_config` String ,
    `create_time` Nullable(DateTime64(3)) ,
    `update_time` Nullable(DateTime64(3)) DEFAULT now64() ,
    `dtu_prefix` String ,
    `max` String DEFAULT '0' ,
    `dtus` String ,
    `platform` String ,
    `sort` Int32 DEFAULT '0' ,
    `channel_code` String DEFAULT '' ,
    `android_monitoring_link` Nullable(String) ,
    `ios_monitoring_link` Nullable(String) ,
    `standard_name` String ,
    `channel_nature` Int16 DEFAULT '0' ,
    `deep_callback_behavior` String DEFAULT '' ,
    `mainbody_config` Nullable(String)
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 2 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE central_qgm_app
(
    `id` Int64,
    `project_id` Int64 DEFAULT '0' ,
    `app_category_id` Nullable(Int32) ,
    `app_group_id` Int32 DEFAULT '0' ,
    `app_name` String DEFAULT '' ,
    `app_key` String DEFAULT '' ,
    `description` String DEFAULT '' ,
    `appid_ios` String DEFAULT '' ,
    `appid_android` String DEFAULT '' ,
    `logo` String DEFAULT '' ,
    `domain` String DEFAULT '' ,
    `status` Int16 DEFAULT '0' ,
    `list_sort` Int32 DEFAULT '0' ,
    `is_inner` Int16 DEFAULT '1' ,
    `operator_id` Int64 DEFAULT '0' ,
    `operator` String DEFAULT '' ,
    `update_time` Nullable(DateTime64(3)) DEFAULT now64() ,
    `create_time` Nullable(DateTime64(3)) DEFAULT now64() ,
    `cid` String DEFAULT '' ,
    `saas_token` String DEFAULT '' ,
    `salt` String DEFAULT '' ,
    `enable_ocpc2` Int16 DEFAULT '0' ,
    `profit_sharing_type` Int16 DEFAULT '0' ,
    `app_type` Int8 DEFAULT '4' ,
    `package_name` Nullable(String) DEFAULT '0' ,
    `template_type` Nullable(String) DEFAULT '' ,
    `app_group_type` Nullable(Int16) DEFAULT '0' ,
    `third_appid` Nullable(String) DEFAULT ''
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 2 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE central_qgm_app_group
(
    `id` Int64,
    `app_group_name` String ,
    `description` String DEFAULT '' ,
    `update_user_id` Int64 DEFAULT '0' ,
    `update_user` String DEFAULT '' ,
    `update_time` Nullable(DateTime64(3)) DEFAULT now64() ,
    `create_time` Nullable(DateTime64(3)) DEFAULT now64()
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 2 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE central_agent
(
    `id` Int64,
    `app_id` Int64 ,
    `raw_id` Int64 ,
    `name` String ,
    `channel_category_ids` String ,
    `channel_ids` String ,
    `company_name` String ,
    `memo` String ,
    `admin_id` Int64 DEFAULT '0' ,
    `admin_name` String ,
    `status` Int8 DEFAULT '0' ,
    `create_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `update_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `sort` Int32 DEFAULT '0' ,
    `contact` String DEFAULT '' ,
    `agent_group_id` Nullable(Int64) DEFAULT '0'
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 2 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';


CREATE TABLE midukyy_full_att_adid_active_daily_report_v1
(
    `id` Int64,
    `date` Date DEFAULT '2018-01-01',
    `campaign_id` String DEFAULT '' ,
    `adgroup_id` String DEFAULT '' ,
    `ad_id` String DEFAULT '' ,
    `dtu` String DEFAULT '',
    `os` Int16 DEFAULT '0' ,
    `active` Int32 DEFAULT '0',
    `yesterday_active` Int32 DEFAULT '0' ,
    `last_week_active` Int32 DEFAULT '0' ,
    `is_hijack` Int16 DEFAULT '0' ,
    `hijack_agent_account_id` Int64 DEFAULT '0' ,
    `create_at` Nullable(DateTime64(3)) DEFAULT now64(),
    `updated_at` Nullable(DateTime64(3)) DEFAULT now64(),
    `agent_account_id` Int64 DEFAULT '0' ,
    `source` String DEFAULT '' ,
    `recall_num` Int64 DEFAULT '0' ,
    `recall_device_num` Int64 DEFAULT '0' ,
    `recall_att_num` Int64 DEFAULT '0' ,
    `recall_click_att_num` Int64 DEFAULT '0' ,
    `attribution_status` Int8 DEFAULT '1' ,
    `last_hourly_active` Int64 DEFAULT '0' ,
    `yesterday_last_hourly_active` Int64 DEFAULT '0' ,
    `last_week_last_hourly_active` Int64 DEFAULT '0'
)
ENGINE = CnchMergeTree
PARTITION BY toYYYYMM(date)
CLUSTER BY id INTO 1 BUCKETS
ORDER BY (id, date)
UNIQUE KEY (id, date)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE midukyy_agent_account_rebate
(
    `id` Int64,
    `agent_account_id` Nullable(Int32) DEFAULT '0',
    `agent_id` Nullable(Int32) DEFAULT '0' ,
    `agent_name` Nullable(String) DEFAULT '' ,
    `opt_agent_id` Nullable(Int32) DEFAULT '0' ,
    `opt_agent_name` Nullable(String) DEFAULT '' ,
    `cur_agent_id` Int64 DEFAULT '0' ,
    `cur_agent_name` Nullable(String) DEFAULT '' ,
    `agent_type` Int16 DEFAULT '0' ,
    `date` Nullable(Date),
    `rate` Nullable(Decimal(10, 2)) DEFAULT '0.00',
    `fees` Nullable(Decimal(10, 2)),
    `update_time` Nullable(DateTime64(3)) DEFAULT now64() ,
    `create_time` Nullable(DateTime64(3)) DEFAULT now64()
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 1 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';


CREATE TABLE midukyy_agent
(
    `id` Int64,
    `name` String ,
    `channel_category_ids` String ,
    `channel_ids` String ,
    `company_name` String ,
    `memo` String ,
    `admin_id` Int64 DEFAULT '0' ,
    `admin_name` String ,
    `status` Int8 DEFAULT '0' ,
    `create_time` Nullable(DateTime64(3)) ,
    `update_time` Nullable(DateTime64(3)) DEFAULT now64() ,
    `sort` Int32 DEFAULT '0' ,
    `contact` String DEFAULT '' ,
    `ref_agent_id` Int64 DEFAULT '0' ,
    `agent_group_id` Nullable(Int64) DEFAULT '0'
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 1 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE midukyy_agent_account
(
    `id` Int64,
    `channel_category_id` Int64 DEFAULT '0' ,
    `channel_id` Int32 DEFAULT '0' ,
    `agent_id` Nullable(Int32) DEFAULT '0' ,
    `agent_name` Nullable(String) DEFAULT '' ,
    `opt_agent_id` Nullable(Int32) DEFAULT '0' ,
    `opt_agent_name` Nullable(String) DEFAULT '' ,
    `username` String ,
    `password` String ,
    `os` Nullable(Int16) DEFAULT '0' ,
    `is_report_active` Int16 DEFAULT '0' ,
    `active_params` String ,
    `rebate_config` String ,
    `wx_mp_name` String ,
    `admin_id` Int64 DEFAULT '0' ,
    `admin_name` String ,
    `status` Int8 DEFAULT '0' ,
    `brand_logo` Nullable(String) ,
    `brand_logo_id` Nullable(String) DEFAULT '' ,
    `is_created_in_ug` Nullable(Int64) DEFAULT '0' ,
    `complete_progress` Nullable(Int64) DEFAULT '0' ,
    `deep_callback_behavior` Nullable(String) DEFAULT '' ,
    `callback_behavior` Nullable(String) DEFAULT '' ,
    `callback_type` Nullable(String) DEFAULT '' ,
    `hw_oaid_ip_key` Nullable(String) DEFAULT '' ,
    `hw_sign` Nullable(String) DEFAULT '' ,
    `akey` Nullable(String) DEFAULT '' ,
    `gdt_ios_data_src_id` Nullable(String) DEFAULT '' ,
    `gdt_android_data_src_id` Nullable(String) DEFAULT '' ,
    `wx_merchant_id` Nullable(String) DEFAULT '' ,
    `wx_ios_id` Nullable(String) DEFAULT '' ,
    `wx_android_id` Nullable(String) DEFAULT '' ,
    `wx_app_key` Nullable(String) DEFAULT '' ,
    `wx_app_id` Nullable(String) DEFAULT '' ,
    `wx_advertiser_id` Nullable(String) DEFAULT '' ,
    `ios_monitor_url` Nullable(String) ,
    `android_monitor_url` Nullable(String) ,
    `api_secret` Nullable(String) ,
    `api_uuid` Nullable(String) ,
    `data_source_id` Nullable(String) ,
    `dsp_grant_status` Nullable(Int16) DEFAULT '0' ,
    `dsp_grant_url` Nullable(String) ,
    `exchange_flow_app_id` Nullable(Int64) DEFAULT '0' ,
    `competition_account_id` Nullable(String) DEFAULT '' ,
    `competition_id` Nullable(String) DEFAULT '' ,
    `activity_name` Nullable(String) DEFAULT '' ,
    `resource_location` Nullable(String) DEFAULT '' ,
    `account_tag` Nullable(String) DEFAULT '' ,
    `growth_type` Nullable(Int16) DEFAULT '1' ,
    `step_complete_status` Nullable(String) ,
    `extra` Nullable(String) ,
    `image_token` Nullable(String) ,
    `intelligent_package_name` Nullable(String) ,
    `user_action_id` Nullable(String) ,
    `promoted_object_id` Nullable(String) ,
    `dsp_avatar_url` Nullable(String) ,
    `dsp_avatar_id` Nullable(String) DEFAULT '' ,
    `account_type` Int8 DEFAULT '1' ,
    `put_type` Int8 DEFAULT '0' ,
    `platform_account_id` Nullable(String) DEFAULT '' ,
    `account_company` Nullable(String) DEFAULT '' ,
    `account_pay_company` Nullable(String) DEFAULT '' ,
    `app_id` Int64 DEFAULT '0' ,
    `create_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `update_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `create_name` Nullable(String) DEFAULT '' ,
    `create_id` Nullable(Decimal(20, 0)) DEFAULT '0' ,
    `bid_creative_id` String DEFAULT '' ,
    `client_id` Nullable(String) DEFAULT '' ,
    `secret_key` Nullable(String) DEFAULT '' ,
    `modifier_id` Nullable(Decimal(20, 0)) DEFAULT '0' ,
    `modifier_name` Nullable(String) DEFAULT '' ,
    `android_monitor_url_v2` Nullable(String) ,
    `ref_agent_account_id` Int64 DEFAULT '0' ,
    `mkt_api_version` Nullable(String) DEFAULT '' ,
    `old_user_switch` Nullable(Int8) DEFAULT '0' ,
    `active_callback_behavior` Nullable(String) DEFAULT '' ,
    `abtest_layer` Nullable(String) DEFAULT ''
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 1 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE midukyy_channel
(
    `id` Int64,
    `channel_category_id` Int64 DEFAULT '0' ,
    `name` String ,
    `url` String ,
    `short_name` String ,
    `admin_id` Decimal(20, 0) DEFAULT '0' ,
    `admin_name` String ,
    `status` Int8 DEFAULT '0' ,
    `spider_config` String ,
    `create_time` Nullable(DateTime64(3)) ,
    `update_time` Nullable(DateTime64(3)) DEFAULT now64() ,
    `dtu_prefix` String ,
    `max` String DEFAULT '0' ,
    `dtus` String ,
    `platform` String ,
    `sort` Int32 DEFAULT '0' ,
    `channel_code` String DEFAULT '' ,
    `android_monitoring_link` Nullable(String) ,
    `ios_monitoring_link` Nullable(String) ,
    `standard_name` String ,
    `channel_nature` Int16 DEFAULT '0' ,
    `deep_callback_behavior` String DEFAULT '' ,
    `ref_channel_id` Int64 DEFAULT '0' ,
    `mainbody_config` Nullable(String)
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 1 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';


CREATE TABLE midukyy_full_att_realtime_std_daily_report_v1
(
    `id` Int64 ,
    `date` Date,
    `agent_account_id` Decimal(20, 0) DEFAULT '0' ,
    `campaign_id` String ,
    `adgroup_id` String ,
    `ad_id` String ,
    `dtu` String DEFAULT '0' ,
    `os` Nullable(Int16) DEFAULT '0' ,
    `active` Decimal(20, 0) ,
    `active_1day` Decimal(20, 0) DEFAULT '0' ,
    `url_report_num` Nullable(Int32) DEFAULT '0' ,
    `cp_report_num` Nullable(Int32) DEFAULT '0' ,
    `start_play_num` Nullable(Int32) DEFAULT '0' ,
    `is_hijack` Int8 DEFAULT '0' ,
    `hijack_agent_account_id` Int64 DEFAULT '0' ,
    `created_at` Nullable(DateTime64(3)) DEFAULT now64(),
    `updated_at` Nullable(DateTime64(3)) DEFAULT now64(),
    `yesterday_active_1day` Int64 DEFAULT '0' ,
    `yesterday_last_hourly_active_1day` Int64 DEFAULT '0' ,
    `last_hourly_active_1day` Int64 DEFAULT '0' ,
    `active_1hour` Decimal(20, 0) ,
    `login_num` Int64 DEFAULT '0' ,
    `member_order_num` Nullable(Int64) DEFAULT '0' ,
    `active_2hour` Decimal(20, 0) ,
    `active_6hour` Int64 DEFAULT '0' ,
    `lowage_tuid_num` Decimal(20, 0) DEFAULT '0' ,
    `app_register_num` Int64 DEFAULT '0' ,
    `create_role_num` Int64 DEFAULT '0' ,
    `new_user_pass_num` Int64 DEFAULT '0' ,
    `key_action_num` Int64 DEFAULT '0' ,
    `first_order_num_t0` Int64 DEFAULT '0' ,
    `first_order_gmv_t0` Int64 DEFAULT '0' ,
    `order_num_t0` Int64 DEFAULT '0' ,
    `order_gmv_t0` Int64 DEFAULT '0' ,
    `repurchase_order_num_t0` Int64 DEFAULT '0' ,
    `refund_order_gmv` Int64 DEFAULT '0' ,
    `refund_order_num` Int64 DEFAULT '0' ,
    `active_cb_num` Int32 DEFAULT '0' ,
    `active_cb_num_yesterday` Int32 DEFAULT '0' ,
    `active_cb_num_last_week` Int32 DEFAULT '0' ,
    `action_cb_num` Int32 DEFAULT '0' ,
    `action_cb_num_yesterday` Int32 DEFAULT '0' ,
    `action_cb_num_last_week` Int32 DEFAULT '0' ,
    `ad_pv` Int64 DEFAULT '0'
)
ENGINE = CnchMergeTree
PARTITION BY toYYYYMM(date)
CLUSTER BY (id, date) INTO 1 BUCKETS
ORDER BY (id, date)
UNIQUE KEY (id, date)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE midukyy_marketing_account_finance_daily_data
(
    `id` Int64,
    `date` Date,
    `agent_account_id` Int32 ,
    `consumption` Decimal(10, 2) DEFAULT '0.00' ,
    `consumption_real` Decimal(10, 2) DEFAULT '0.00' ,
    `cash_consumption` Decimal(10, 2) DEFAULT '0.00' ,
    `cash_consumption_real` Decimal(10, 2) DEFAULT '0.00' ,
    `grant_consumption` Decimal(10, 2) DEFAULT '0.00' ,
    `grant_consumption_real` Decimal(10, 2) DEFAULT '0.00' ,
    `payback` Decimal(10, 2) DEFAULT '0.00' ,
    `create_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `update_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `contract_rebate_consumption` Nullable(Decimal(10, 2)) DEFAULT '0.00' ,
    `contract_rebate_consumption_real` Nullable(Decimal(10, 2)) DEFAULT '0.00'
)
ENGINE = CnchMergeTree
PARTITION BY toYYYYMM(date)
CLUSTER BY id INTO 1 BUCKETS
ORDER BY (id, date)
UNIQUE KEY (id, date)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE midukyy_marketing_daily_data_v2
(
    `id` Int64,
    `agent_id` Int64 DEFAULT '0' ,
    `agent_name` String DEFAULT '' ,
    `channel_id` Int64 DEFAULT '0' ,
    `channel_name` String DEFAULT ' ' ,
    `agent_account_id` Int64 DEFAULT '0' ,
    `username` String DEFAULT '' ,
    `date` Date ,
    `campaign` String DEFAULT ' ' ,
    `campaign_id` String DEFAULT '0' ,
    `campaign_budget` Decimal(10, 2) DEFAULT '0.00' ,
    `adgroup` String DEFAULT ' ' ,
    `adgroup_id` String DEFAULT '0' ,
    `adgroup_budget` Decimal(10, 2) DEFAULT '0.00',
    `ad` String DEFAULT ' ' ,
    `ad_id` String DEFAULT '0' ,
    `material_type` Int16 DEFAULT '0' ,
    `material_id` String DEFAULT '' ,
    `material_url` String DEFAULT '' ,
    `material_md5` String DEFAULT '' ,
    `material_title` String DEFAULT '' ,
    `material_created_date` Nullable(Date) ,
    `landing_page_id` String DEFAULT '' ,
    `landing_page_url` String DEFAULT '' ,
    `landing_page_md5` String DEFAULT '' ,
    `slot_id` String DEFAULT '' ,
    `slot_name` String DEFAULT '' ,
    `slot_desc` String DEFAULT '' ,
    `slot_type` String DEFAULT '' ,
    `impression` Int64 DEFAULT '0' ,
    `impression_total` Nullable(Int32) DEFAULT '0' ,
    `click` Int64 DEFAULT '0' ,
    `click_total` Nullable(Int32) DEFAULT '0' ,
    `consumption` Decimal(10, 2) ,
    `consumption_real` Nullable(Decimal(10, 2)) DEFAULT '0.00' ,
    `rebate` Nullable(Decimal(5, 2)) DEFAULT '0.00' ,
    `consumption_total` Nullable(Decimal(10, 2)) DEFAULT '0.00' ,
    `opt_targets` Nullable(String) DEFAULT '' ,
    `billing_type` Nullable(String) DEFAULT '' ,
    `targets_cost` Nullable(Decimal(8, 2)) DEFAULT '0.00' ,
    `install_num` Nullable(Int32) DEFAULT '0' ,
    `put_type` Nullable(String) DEFAULT '' ,
    `active_internal` Nullable(Int32) DEFAULT '0' ,
    `action_click` Nullable(Int32) DEFAULT '0' ,
    `share_num` Nullable(Int32) DEFAULT '0' ,
    `comment_num` Nullable(Int32) DEFAULT '0' ,
    `accusation_num` Nullable(Int32) DEFAULT '0' ,
    `praised_num` Nullable(Int32) DEFAULT '0' ,
    `new_att_num` Nullable(Int32),
    `blacklist_num` Nullable(Int32) DEFAULT '0' ,
    `lessen_num` Nullable(Int32) DEFAULT '0' ,
    `android_download_start` Nullable(Int32) DEFAULT '0' ,
    `android_download_finished` Nullable(Int32) DEFAULT '0' ,
    `download` Int64 DEFAULT '0' ,
    `download_total` Nullable(Int32) DEFAULT '0' ,
    `active` Int64 DEFAULT '0' ,
    `active_7day` Int64 DEFAULT '0' ,
    `active_total` Nullable(Int32) DEFAULT '0' ,
    `action_bar_click` Int32 DEFAULT '0' ,
    `action_bar_click_total` Nullable(Int32) DEFAULT '0' ,
    `retention_num` Int32 DEFAULT '0' ,
    `retention_num_total` Nullable(Int32) DEFAULT '0' ,
    `daily_budget` Decimal(11, 2) DEFAULT '0.00' ,
    `account_balance` Decimal(11, 2) DEFAULT '0.00' ,
    `second_click` Int64 DEFAULT '0' ,
    `second_click_total` Nullable(Int32) DEFAULT '0' ,
    `slot` String DEFAULT '',
    `dtu` String DEFAULT '',
    `create_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `update_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `os` Nullable(Int8) DEFAULT '0' ,
    `cover_img_impression` Nullable(Decimal(20, 0)) DEFAULT '0' ,
    `cover_img_click` Nullable(Decimal(20, 0)) DEFAULT '0' ,
    `yesterday_active` Int64 DEFAULT '0' ,
    `last_week_active` Int64 DEFAULT '0' ,
    `yesterday_consumption` Decimal(11, 2) DEFAULT '0.00' ,
    `last_week_consumption` Decimal(11, 2) DEFAULT '0.00' ,
    `yesterday_consumption_real` Decimal(10, 2) DEFAULT '0.00' ,
    `last_week_consumption_real` Decimal(10, 2) DEFAULT '0.00' ,
    `material_impression` Decimal(20, 0) DEFAULT '0' ,
    `material_click` Decimal(20, 0) DEFAULT '0' ,
    `yesterday_click` Int64 DEFAULT '0' ,
    `yesterday_impression` Int64 DEFAULT '0' ,
    `last_week_click` Int64 DEFAULT '0' ,
    `last_week_impression` Int64 DEFAULT '0' ,
    `last_hourly_consumption` Int64 DEFAULT '0' ,
    `yesterday_last_hourly_consumption` Int64 DEFAULT '0' ,
    `last_week_last_hourly_consumption` Int64 DEFAULT '0' ,
    `last_hour_consumption_real` Int64 DEFAULT '0' ,
    `yesterday_last_hourly_consumption_real` Int64 DEFAULT '0' ,
    `last_week_last_hourly_consumption_real` Int64 DEFAULT '0' ,
    `last_hourly_consumption_v2` Decimal(11, 2) DEFAULT '0.00' ,
    `last_hourly_consumption_real_v2` Decimal(11, 2) DEFAULT '0.00' ,
    `yesterday_last_hourly_consumption_v2` Decimal(11, 2) DEFAULT '0.00' ,
    `yesterday_last_hourly_consumption_real_v2` Decimal(11, 2) DEFAULT '0.00' ,
    `last_week_last_hourly_consumption_v2` Decimal(11, 2) DEFAULT '0.00' ,
    `last_week_last_hourly_consumption_real_v2` Decimal(11, 2) DEFAULT '0.00' ,
    `payback` Nullable(Decimal(10, 2)) DEFAULT '0.00' ,
    `valid_play` Int64 DEFAULT '0' ,
    `yesterday_material_click` Int64 DEFAULT '0',
    `yesterday_material_impression` Int64 DEFAULT '0',
    `yesterday_cover_click` Int64 DEFAULT '0',
    `yesterday_cover_impression` Int64 DEFAULT '0',
    `last_week_material_click` Int64 DEFAULT '0',
    `last_week_material_impression` Int64 DEFAULT '0',
    `last_week_cover_click` Int64 DEFAULT '0',
    `last_week_cover_impression` Int64 DEFAULT '0',
    `yesterday_last_hourly_material_click` Int64 DEFAULT '0',
    `yesterday_last_hourly_material_impression` Int64 DEFAULT '0',
    `yesterday_last_hourly_cover_click` Int64 DEFAULT '0',
    `yesterday_last_hourly_cover_impression` Int64 DEFAULT '0',
    `yesterday_last_hourly_click` Int64 DEFAULT '0',
    `yesterday_last_hourly_impression` Int64 DEFAULT '0',
    `last_week_last_hourly_material_click` Int64 DEFAULT '0',
    `last_week_last_hourly_material_impression` Int64 DEFAULT '0',
    `last_week_last_hourly_cover_click` Int64 DEFAULT '0',
    `last_week_last_hourly_cover_impression` Int64 DEFAULT '0',
    `last_week_last_hourly_click` Int64 DEFAULT '0',
    `last_week_last_hourly_impression` Int64 DEFAULT '0',
    `deep_convert` Int32 DEFAULT '0' ,
    `play_3s` Int32 DEFAULT '0' ,
    `convert` Int32 DEFAULT '0' ,
    `outer_pay_amount` Nullable(Decimal(10, 2)) DEFAULT '0.00'
)
ENGINE = CnchMergeTree
PARTITION BY toYYYYMM(date)
CLUSTER BY id INTO 1 BUCKETS
ORDER BY (id, date)
UNIQUE KEY (id, date)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE central_agent_account
(
    `id` Int64,
    `app_id` Int64 ,
    `raw_id` Int64 ,
    `channel_category_id` Int64 DEFAULT '0' ,
    `channel_id` Int32 DEFAULT '0' ,
    `agent_id` Nullable(Int32) DEFAULT '0' ,
    `agent_name` Nullable(String) DEFAULT '' ,
    `opt_agent_id` Nullable(Int32) DEFAULT '0' ,
    `opt_agent_name` Nullable(String) DEFAULT '' ,
    `username` String ,
    `password` String ,
    `os` Nullable(Int16) DEFAULT '0' ,
    `is_report_active` Int16 DEFAULT '0' ,
    `active_params` String ,
    `rebate_config` String ,
    `wx_mp_name` String ,
    `admin_id` Int64 DEFAULT '0' ,
    `admin_name` String ,
    `status` Int8 DEFAULT '0' ,
    `brand_logo` Nullable(String) ,
    `brand_logo_id` Nullable(String) DEFAULT '' ,
    `is_created_in_ug` Nullable(Int64) DEFAULT '0' ,
    `complete_progress` Nullable(Int64) DEFAULT '0' ,
    `deep_callback_behavior` Nullable(String) DEFAULT '' ,
    `callback_behavior` Nullable(String) DEFAULT '' ,
    `callback_type` Nullable(String) DEFAULT '' ,
    `hw_oaid_ip_key` Nullable(String) DEFAULT '' ,
    `hw_sign` Nullable(String) DEFAULT '' ,
    `akey` Nullable(String) DEFAULT '' ,
    `gdt_ios_data_src_id` Nullable(String) DEFAULT '' ,
    `gdt_android_data_src_id` Nullable(String) DEFAULT '' ,
    `wx_merchant_id` Nullable(String) DEFAULT '' ,
    `wx_ios_id` Nullable(String) DEFAULT '' ,
    `wx_android_id` Nullable(String) DEFAULT '' ,
    `wx_app_key` Nullable(String) DEFAULT '' ,
    `wx_app_id` Nullable(String) DEFAULT '' ,
    `wx_advertiser_id` Nullable(String) DEFAULT '' ,
    `ios_monitor_url` Nullable(String) ,
    `android_monitor_url` Nullable(String) ,
    `dsp_grant_status` Nullable(Int16) DEFAULT '0' ,
    `dsp_grant_url` Nullable(String) ,
    `exchange_flow_app_id` Nullable(Int64) DEFAULT '0' ,
    `competition_account_id` Nullable(String) DEFAULT '' ,
    `competition_id` Nullable(String) DEFAULT '' ,
    `activity_name` Nullable(String) DEFAULT '' ,
    `resource_location` Nullable(String) DEFAULT '' ,
    `account_tag` Nullable(String) DEFAULT '' ,
    `growth_type` Nullable(Int16) DEFAULT '1' ,
    `step_complete_status` Nullable(String) ,
    `extra` Nullable(String) ,
    `image_token` Nullable(String) ,
    `intelligent_package_name` Nullable(String) ,
    `user_action_id` Nullable(String) ,
    `promoted_object_id` Nullable(String) ,
    `dsp_avatar_url` Nullable(String) ,
    `dsp_avatar_id` Nullable(String) DEFAULT '' ,
    `account_type` Int8 DEFAULT '1' ,
    `put_type` Int8 DEFAULT '0' ,
    `platform_account_id` Nullable(String) DEFAULT '' ,
    `account_company` Nullable(String) DEFAULT '' ,
    `account_pay_company` Nullable(String) DEFAULT '' ,
    `create_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `update_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `is_outer` Int8 DEFAULT '0' ,
    `android_monitor_url_v2` Nullable(String) ,
    `mkt_api_version` Nullable(String) DEFAULT '' ,
    `old_user_switch` Nullable(Int8) DEFAULT '0' ,
    `active_callback_behavior` Nullable(String) DEFAULT '' ,
    `abtest_layer` Nullable(String) DEFAULT ''
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 2 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

CREATE TABLE central_channel
(
    `id` Int64,
    `app_id` Int64 ,
    `raw_id` Int64 ,
    `channel_category_id` Int64 DEFAULT '0' ,
    `name` String ,
    `url` String ,
    `short_name` String ,
    `admin_id` Decimal(20, 0) DEFAULT '0' ,
    `admin_name` String ,
    `status` Int8 DEFAULT '0' ,
    `spider_config` String ,
    `create_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `update_time` Nullable(DateTime64(3)) DEFAULT now64(),
    `dtu_prefix` String ,
    `max` String DEFAULT '0' ,
    `dtus` String ,
    `platform` String ,
    `sort` Int32 DEFAULT '0' ,
    `channel_code` String DEFAULT '' ,
    `android_monitoring_link` Nullable(String) ,
    `ios_monitoring_link` Nullable(String) ,
    `deep_callback_behavior` String DEFAULT '' ,
    `channel_nature` Int16 DEFAULT '1' ,
    `standard_name` String ,
    `mainbody_config` Nullable(String)
)
ENGINE = CnchMergeTree
PARTITION BY intDiv(id, 18446744073709551)
CLUSTER BY id INTO 2 BUCKETS
ORDER BY tuple(id)
UNIQUE KEY tuple(id)
SETTINGS allow_nullable_key = 1, storage_dialect_type = 'MYSQL';

SET dialect_type = 'MYSQL';
SET allow_mysql_having_name_resolution = 1;


-- SELECT
--     aadr.date,
--     qa.id AS app_id,
--     qa.app_key AS app_key,
--     qa.app_name AS app_name,
--     qa.profit_sharing_type AS profit_sharing_type,
--     qa.logo AS app_logo,
--     qag.id AS app_group_id,
--     qag.app_group_name AS app_group_name,
--     qa.app_category_id AS app_category_id,
--     IFNULL(ugc.id, 0) AS ug_channel_id,
--     IFNULL(uga.id, 0) AS ug_agent_id,
--     IFNULL(ugaa.id, 0) AS ug_agent_account_id,
--     ifnull(c.id, 0) AS channel_id,
--     ifnull(c.name, '') AS channel_name,
--     ifnull(c.channel_nature, '') AS channel_nature,
--     ifnull(c.channel_code, '') AS channel_code,
--     ifnull(aa.username, '') AS agent_account_name,
--     ifnull(aa.os, 0) AS agent_account_os,
--     ifnull(aa.account_type, 1) AS account_type,
--     ifnull(aa.account_tag, '') AS account_tag,
--     ifnull(aa.account_company, '') AS account_company,
--     ifnull(a.id, 0) AS agent_id,
--     ifnull(a.name, '') AS agent_name,
--     '' AS att_campaign_id,
--     '' AS att_adgroup_id,
--     '' AS att_ad_id,
--     0 AS agent_id,
--     aadr.agent_account_id,
--     aadr.grant_consumption_real,
--     aadr.grant_consumption,
--     aadr.contract_rebate_consumption,
--     aadr.contract_rebate_consumption_real
-- FROM adongxcxmdext_marketing_account_finance_daily_data AS aadr
-- LEFT JOIN adongxcxmdext_agent_account_rebate AS aar ON (aar.date = aadr.date) AND (aadr.agent_account_id = aar.agent_account_id)
-- LEFT JOIN adongxcxmdext_agent AS a ON aar.cur_agent_id = a.id
-- LEFT JOIN adongxcxmdext_agent_account AS aa ON aa.id = aadr.agent_account_id
-- LEFT JOIN adongxcxmdext_channel AS c ON aa.channel_id = c.id
-- LEFT JOIN central_qgm_app AS qa ON qa.app_key = 'adongxcxmdext'
-- LEFT JOIN central_qgm_app_group AS qag ON qa.app_group_id = qag.id
-- LEFT JOIN central_agent AS uga ON (aar.cur_agent_id = uga.raw_id) AND (qa.id = uga.app_id)
-- LEFT JOIN central_agent_account AS ugaa ON (aadr.agent_account_id = ugaa.raw_id) AND (qa.id = ugaa.app_id)
-- LEFT JOIN central_channel AS ugc ON (ugaa.channel_id = ugc.raw_id) AND (qa.id = ugc.app_id)
-- WHERE aadr.date = '2024-05-09'
-- HAVING (aadr.grant_consumption_real > 0) OR (aadr.grant_consumption > 0) OR (aadr.contract_rebate_consumption > 0) OR (aadr.contract_rebate_consumption_real > 0);


SELECT
    main.date AS date,
    sum(main.consumption_real) AS consumption_real,
    sum(main.outer_active) AS outer_active,
    IF(sum(main.outer_active) != 0, sum(main.consumption_real) / sum(main.outer_active), 0) AS outer_cpa,
    IF(SUM(main.consumption_real) != 0, (SUM(main.total_revenue_real) / 100) / ((SUM(main.consumption_real) - SUM(main.grant_consumption_real)) - SUM(main.contract_rebate_consumption_real)), 0) AS rroi_roi,
    sum(ifnull(main.total_revenue_real, 0)) / 100 AS total_revenue_real,
    sum(main.material_impression) AS material_impression
FROM
(
    SELECT
        aadr.date AS date,
        mdd.consumption_real AS consumption_real,
        mdd.active AS outer_active,
        if(isNull(maf.grant_consumption_real), 0, maf.grant_consumption_real) AS grant_consumption_real,
        if(isNull(maf.contract_rebate_consumption_real), 0, maf.contract_rebate_consumption_real) AS contract_rebate_consumption_real,
        multiIf((LOWER(aadr.dtu) = 'vivo') AND (aa.abtest_layer = 'tt1.0'), frsdr.order_gmv_t0 * 1.0278112472366534, (LOWER(aadr.dtu) = 'vivo') AND (aa.abtest_layer = 'tt2.0'), frsdr.order_gmv_t0 * 0.993498050244303, (LOWER(aadr.dtu) = 'oppo') AND (aa.abtest_layer = 'tt3.0'), frsdr.order_gmv_t0 * 1.054190574070775, (LOWER(aadr.dtu) = 'oppo') AND (aa.abtest_layer = 'tt4.0'), frsdr.order_gmv_t0 * 1.042407686639331, (LOWER(aadr.dtu) = 'oppo') AND (aa.abtest_layer = 'tt2.0'), frsdr.order_gmv_t0 * 1.0632433732622883, (LOWER(aadr.dtu) = 'oppo') AND (aa.abtest_layer = 'tt1.0'), frsdr.order_gmv_t0 * 1.177272360714721, (LOWER(aadr.dtu) = 'oppo') AND (aa.abtest_layer = 'oppo_lock'), frsdr.order_gmv_t0 * 1.3411268943776988, (LOWER(aadr.dtu) = 'vivo') AND (aa.abtest_layer = 'tt4.0'), frsdr.order_gmv_t0 * 1.1635967616901213, (LOWER(aadr.dtu) = 'oppo') AND (aa.abtest_layer = 'oppo_feed'), frsdr.order_gmv_t0 * 1.2838990478582595, (LOWER(aadr.dtu) = 'vivo') AND (aa.abtest_layer = 'tt3.0'), frsdr.order_gmv_t0 * 1.0796519303013554, frsdr.order_gmv_t0 * 1) AS total_revenue_real,
        mdd.material_impression AS material_impression
    FROM midukyy_full_att_adid_active_daily_report_v1 AS aadr
    LEFT JOIN midukyy_agent_account_rebate AS aar ON (aar.date = aadr.date) AND (aadr.agent_account_id = aar.agent_account_id)
    LEFT JOIN midukyy_agent AS a ON aar.cur_agent_id = a.id
    LEFT JOIN midukyy_agent_account AS aa ON aa.id = aadr.agent_account_id
    LEFT JOIN midukyy_channel AS c ON aa.channel_id = c.id
    LEFT JOIN midukyy_marketing_daily_data_v2 AS mdd ON (aadr.date = mdd.date) AND (aadr.agent_account_id = mdd.agent_account_id) AND (LOWER(aadr.dtu) = LOWER(mdd.dtu)) AND (aadr.campaign_id = mdd.campaign_id) AND (aadr.adgroup_id = mdd.adgroup_id) AND (aadr.ad_id = mdd.ad_id)
    LEFT JOIN midukyy_full_att_realtime_std_daily_report_v1 AS frsdr ON (aadr.date = frsdr.date) AND (aadr.agent_account_id = frsdr.agent_account_id) AND (LOWER(aadr.dtu) = LOWER(frsdr.dtu)) AND (aadr.campaign_id = frsdr.campaign_id) AND (aadr.adgroup_id = frsdr.adgroup_id) AND (aadr.ad_id = frsdr.ad_id)
    LEFT JOIN midukyy_marketing_account_finance_daily_data AS maf ON (aadr.date = maf.date) AND (aadr.agent_account_id = maf.agent_account_id) AND (aadr.campaign_id = '-1') AND (aadr.adgroup_id = '-1') AND (aadr.ad_id = '-1')
    WHERE (aadr.date = '2024-05-09') AND (aadr.date = '2024-05-09')
    HAVING (consumption_real > 0) OR (outer_active > 0) OR (grant_consumption_real > 0) OR (contract_rebate_consumption_real > 0) OR (total_revenue_real > 0) OR (material_impression > 0)
) AS main
GROUP BY main.date
ORDER BY
    main.date DESC,
    material_impression DESC
LIMIT 0, 50;


DROP TABLE IF EXISTS adongxcxmdext_marketing_account_finance_daily_data ;
DROP TABLE IF EXISTS adongxcxmdext_agent_account_rebate ;
DROP TABLE IF EXISTS adongxcxmdext_agent ;
DROP TABLE IF EXISTS adongxcxmdext_agent_account ;
DROP TABLE IF EXISTS adongxcxmdext_channel ;
DROP TABLE IF EXISTS central_qgm_app ;
DROP TABLE IF EXISTS central_qgm_app_group ;
DROP TABLE IF EXISTS central_agent;
DROP TABLE IF EXISTS central_agent_account;
DROP TABLE IF EXISTS central_channel;
DROP TABLE IF EXISTS midukyy_full_att_adid_active_daily_report_v1;
DROP TABLE IF EXISTS midukyy_agent_account_rebate;
DROP TABLE IF EXISTS midukyy_agent;
DROP TABLE IF EXISTS midukyy_agent_account;
DROP TABLE IF EXISTS midukyy_channel;
DROP TABLE IF EXISTS midukyy_full_att_realtime_std_daily_report_v1;
DROP TABLE IF EXISTS midukyy_marketing_account_finance_daily_data;
DROP TABLE IF EXISTS midukyy_marketing_daily_data_v2;
DROP TABLE IF EXISTS central_agent_account;
DROP TABLE IF EXISTS central_channel;
