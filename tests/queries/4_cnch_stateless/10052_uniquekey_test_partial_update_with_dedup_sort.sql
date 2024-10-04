DROP TABLE IF EXISTS unique_partial_update_with_dedup_sort;

CREATE TABLE unique_partial_update_with_dedup_sort
(
  `event_time` DateTime,
  `product_id` UInt64,
  `city` String,
  `category` String,
  `amount` UInt32,
  `revenue` UInt64
)
ENGINE = CnchMergeTree(event_time)
ORDER BY city
UNIQUE KEY product_id 
SETTINGS enable_unique_partial_update = 1, partial_update_enable_merge_map = 0;

SET enable_staging_area_for_write=0, enable_unique_partial_update = 1;
INSERT INTO unique_partial_update_with_dedup_sort (event_time, product_id, city, category, amount, revenue, _update_columns_) VALUES('2020-10-29 23:40:00', 10001, 'Beijing', '男装', 5, 500, 'event_time,product_id,city,category,amount,revenue'),('2020-10-29 23:40:00', 10002, 'Beijing', '男装', 2, 200, 'event_time,product_id,city,category,amount,revenue'),('2020-10-29 23:50:00', 10001, 'Shanghai', '童装', 8, 800, 'event_time,product_id,city'),('2020-10-29 23:50:00', 10002, 'Shanghai', '童装', 5, 500, 'event_time,product_id,city');

SELECT * FROM unique_partial_update_with_dedup_sort;

DROP TABLE IF EXISTS unique_partial_update_with_dedup_sort;
