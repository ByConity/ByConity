DROP TABLE IF EXISTS tag_bitmaps_did_cdp_local;

CREATE TABLE tag_bitmaps_did_cdp_local
(
    `id_type` Int32,
    `id_map` Array(UInt64),
    `tag_value` String,
    `p_date` Date,
    `tag_value_double` Float64,
    `tag_type` Int8,
    `tag_id` Int32,
    `split_id` Int32,
    `id_map_cnt` UInt64,
    `app_id` Int32
)
ENGINE = CnchMergeTree()
PARTITION BY (p_date, tag_id, tag_type, split_id, id_type)
ORDER BY (tag_value, tag_value_double, cityHash64(tag_value))
SAMPLE BY cityHash64(tag_value)
SETTINGS index_granularity = 128;

INSERT INTO tag_bitmaps_did_cdp_local VALUES (1,[1,2],'a','2023-05-06',1.33,1,22,1,1,1);

SELECT * FROM tag_bitmaps_did_cdp_local WHERE (p_date = '2023-05-06') AND (tag_type = 1) AND (id_type = 1) AND (tag_id = 22) AND toInt64(tag_value_double) = 1.33;

DROP TABLE IF EXISTS tag_bitmaps_did_cdp_local;
