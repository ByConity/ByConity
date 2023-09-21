DROP TABLE IF EXISTS merge_agg_thread_test_bitmap;

-- use bitmap to test because one row could be large.
CREATE TABLE merge_agg_thread_test_bitmap (id_map BitMap64, partition_id UInt64, split_id UInt64) ENGINE = CnchMergeTree() PARTITION BY `partition_id` ORDER BY `split_id`;

-- insert about numbers(100*1000*1000) if need test performance
INSERT INTO merge_agg_thread_test_bitmap
SELECT arrayToBitmap(groupArray(id)) AS id_map, partition_id, split_id FROM (
    SELECT number*100 as id, number % 100 as partition_id, number % 40 as split_id FROM numbers(100*1000)
) GROUP BY partition_id, split_id;

SELECT sum(bitmapCardinality(id_map0))
FROM (
    SELECT bitmapColumnOr(id_map) AS id_map0
    FROM merge_agg_thread_test_bitmap
    GROUP BY split_id
);

SELECT sum(bitmapCardinality(id_map0))
FROM (
    SELECT bitmapColumnOr(id_map) AS id_map0
    FROM merge_agg_thread_test_bitmap
    GROUP BY split_id
) SETTINGS merge_agg_use_multi_thread_threshold_rows = 10;
