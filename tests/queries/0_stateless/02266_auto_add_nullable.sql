SET
    allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS 02266_auto_add_nullable;

CREATE TABLE 02266_auto_add_nullable (
    val0 Int8 DEFAULT NULL,
    val1 Nullable(Int8) DEFAULT NULL,
    val2 UInt8 DEFAULT NUll,
    val3 String DEFAULT null,
    val4 LowCardinality(Int8) DEFAULT NULL,
    val5 LowCardinality(Nullable(Int8)) DEFAULT NULL,
    val6 Array(Int8) DEFAULT NULL
) ENGINE = MergeTree
order by tuple();

DESCRIBE TABLE 02266_auto_add_nullable;

DROP TABLE IF EXISTS 02266_auto_add_nullable;

DROP TABLE IF EXISTS landing_page;

CREATE TABLE IF NOT EXISTS landing_page (
    id Int64,
    name String,
    title String,
    token String,
    created_at DateTime,
    updated_at DateTime,
    version Int32 default 0,
    content String,
    landing_page_width Int32,
    landing_page_height Int32,
    landing_page_size Float64,
    landing_page_group_id Int32 default 0,
    advertiser_account_group_id Int64,
    delete_status Int32 default 0,
    landing_page_type Int32 default 0,
    wechat_customer_service_group_id Int64 default null,
    wechat_official_account_id Int64 default null,
    landing_page_wechat_customer_city_code_id Int64 default null,
    wechat_group_chat_group_id Int64 default null,
    landing_page_wechat_group_chat_city_code_id Int64 default null,
    qr_code_url String default null,
    review Int64 default null
) ENGINE = PostgreSQL(
    'xxx',
    'xxx',
    'xxx',
    'xxx',
    'xxx',
    'xxx'
) comment '落地页表';

DESCRIBE TABLE landing_page;

DROP TABLE IF EXISTS landing_page;