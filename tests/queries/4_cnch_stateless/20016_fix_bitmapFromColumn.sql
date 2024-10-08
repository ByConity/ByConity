CREATE TABLE test_detail_table
(
    `id` Int64,
    `tag` String,
    `p_date` Date,
    `slice_id` UInt64
)
ENGINE = CnchMergeTree
PARTITION BY p_date
CLUSTER BY EXPRESSION slice_id INTO 10 BUCKETS
ORDER BY (tag, id);

insert into test_detail_table select number as id, toString(number%5) as tag, '2024-08-01' as p_date, number%10 as slice_id from numbers(1000);

select bitmapCardinality(bitmapFromColumn(id)) as ids from test_detail_table where p_date='2024-08-01' and slice_id=2;

select bitmapCardinality(ids) as res from (select bitmapFromColumn(id) as ids from test_detail_table where p_date='2024-08-01');