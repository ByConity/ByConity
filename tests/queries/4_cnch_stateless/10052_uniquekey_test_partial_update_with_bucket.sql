DROP TABLE IF EXISTS unique_partial_update_with_bucket;

CREATE TABLE unique_partial_update_with_bucket
(
    `event_time` DateTime,
    `product_id` UInt64,
    `made_in_city` Array(String),
    `amount` UInt64,
    `broad` String,
    `category` Nullable(String),
    `send_to` LowCardinality(String),
    `customer` Map(String, String)
)
ENGINE = CnchMergeTree
PARTITION BY toDate(event_time)
ORDER BY (made_in_city)
CLUSTER BY category INTO 5 BUCKETS
UNIQUE KEY product_id
SETTINGS enable_unique_partial_update = 1, partial_update_enable_merge_map = 0;

SET enable_unique_partial_update = 1;

SELECT 'Partial update with bucket test, stage 1';
INSERT INTO unique_partial_update_with_bucket (event_time, product_id, made_in_city, category, send_to, amount, broad, customer) VALUES ('2020-10-29 23:50:00', 10001, ['Beijing'], '男装', '北京', 5, '奥克斯', {'消费者k-1': '消费者v-1'}), ('2020-10-29 23:50:00', 10002, ['Beijing'], '男装', '北京', 5, '奥克斯', {'消费者k-2': '消费者v-2'}), ('2020-10-29 23:50:00', 10003, ['Beijing'], '男装', '北京', 5, '奥克斯', {'消费者k-3': '消费者v-3'}), ('2020-10-29 23:50:00', 10004, ['Beijing'], '男装', '北京', 5, '奥克斯', {'消费者k-4': '消费者v-4'});
SELECT * FROM unique_partial_update_with_bucket order by product_id;

SELECT 'Partial update with bucket test, stage 2';
INSERT INTO unique_partial_update_with_bucket (event_time, product_id, made_in_city, category, send_to, amount, broad, customer, _update_columns_, _delete_flag_) VALUES ('2020-10-29 23:50:00', 10001, ['Shanghai'], '童装', '上海', 50, 'OPPO', {'消费者k-10': '消费者v-10'}, '', 0), ('2020-10-29 23:50:00', 10002, ['Shanghai'], '女装', '上海', 50, 'VIVO', {'消费者k-20': '消费者v-20'}, '', 0),  ('2020-10-29 23:50:00', 10003, ['Shanghai'], '牛仔', '上海', 50, '大疆', {'消费者k-30': '消费者v-30'}, '', 0);
SELECT * FROM unique_partial_update_with_bucket order by product_id;

DROP TABLE IF EXISTS unique_partial_update_with_bucket;
