drop table if exists test_tag_bitmap_20027;
drop table if exists test_tag_bitmap_v2_20027;

CREATE TABLE test_tag_bitmap_20027(
    `tag_id` Int32,
    `uids` BitMap64,
    `p_date` Date
)ENGINE = CnchMergeTree
PARTITION BY p_date
ORDER BY tag_id;

insert into test_tag_bitmap_20027 values (2, [1,2,3,4,5],'2024-01-01'),(5,[3,4,5,6,7,8],'2024-01-01');

select 'single_agg', bitmapCount('2&5')(tag_id, uids) from test_tag_bitmap_20027 where p_date = '2024-01-01';

select 'multi_agg', bitmapCount('5')(tag_id, uids), bitmapCount('2')(tag_id, uids), bitmapCount('2&5')(tag_id, uids), bitmapMultiCount('5','2','2&5')(tag_id, uids) from test_tag_bitmap_20027 where p_date = '2024-01-01';

-- no predicate optimization now
select 'multi_agg_with_subquery', bitmapCount('5')(tag_id, uids), bitmapCount('2')(tag_id, uids), bitmapCount('2&5')(tag_id, uids), bitmapMultiCount('5','2','2&5')(tag_id, uids) from ( select tag_id, uids from test_tag_bitmap_20027 where p_date = '2024-01-01');

CREATE TABLE test_tag_bitmap_v2_20027 (
f1 Int32, f2 UInt64, f3 String, uids BitMap64, p_date Date
) engine=CnchMergeTree PARTITION BY p_date ORDER BY f1;

insert into test_tag_bitmap_v2_20027 values (1, 2, 'a', [1,2,3,4,5], '2024-01-01'),(2,2,'b',[3,4,5,6], '2024-01-01'),(3,1,'c',[1,2,4,8],'2024-01-01');

--- new where condition: WHERE ((f3 IN ('a', 'b')) OR (f1 IN (1, 2, 3))) AND (p_date = '2024-01-01')
select 'multi_agg_multi_column',
bitmapCount('1')(f1, uids) as r1,
bitmapMultiCount('1|2','2','3')(f1, uids) as r2,
bitmapCount('a')(f3, uids) as r3,
bitmapCount('a&b')(f3, uids) as r4,
bitmapCount('2')(toInt64(f2), uids) as r5,
bitmapCount('2~1')(toInt64(f2), uids) as r6
from test_tag_bitmap_v2_20027 where p_date='2024-01-01';

-- no predicate optimization now
select 'multi_agg_multi_column_with_subquery',
bitmapCount('1')(f1, uids) as r1,
bitmapMultiCount('1|2','2','3')(f1, uids) as r2,
bitmapCount('a')(f3, uids) as r3,
bitmapCount('a&b')(f3, uids) as r4,
bitmapCount('2')(toInt64(f2), uids) as r5,
bitmapCount('2~1')(toInt64(f2), uids) as r6
FROM
(
    select *
    from test_tag_bitmap_v2_20027 where p_date='2024-01-01'
);

SELECT 'multi_agg_multi_column_2', bitmapExtract('1')(f1, uids) AS r1, bitmapMultiExtract('1|2', '2', '3')(f1, uids) AS r2, bitmapExtract('a')(f3, uids) AS r3, bitmapExtract('a&b')(f3, uids) AS r4, bitmapExtract('2')(toInt64(f2), uids) AS r5, bitmapExtract('2~1')(toInt64(f2), uids) AS r6 FROM test_tag_bitmap_v2_20027 WHERE p_date='2024-01-01';

-- no predicate optimization now
SELECT 'multi_agg_multi_column_2_with_subquery', bitmapExtract('1')(f1, uids) AS r1, bitmapMultiExtract('1|2', '2', '3')(f1, uids) AS r2, bitmapExtract('a')(f3, uids) AS r3, bitmapExtract('a&b')(f3, uids) AS r4, bitmapExtract('2')(toInt64(f2), uids) AS r5, bitmapExtract('2~1')(toInt64(f2), uids) AS r6 FROM ( select * FROM test_tag_bitmap_v2_20027 WHERE p_date='2024-01-01');

drop table test_tag_bitmap_20027;
drop table test_tag_bitmap_v2_20027;