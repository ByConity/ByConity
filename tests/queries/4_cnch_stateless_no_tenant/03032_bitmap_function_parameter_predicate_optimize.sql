drop table if exists test_tag_bitmap;
drop table if exists test_tag_bitmap_v2;

set extract_bitmap_implicit_filter = 1;

CREATE TABLE test_tag_bitmap(
    `tag_id` Int32,
    `uids` BitMap64,
    `p_date` Date
)ENGINE = CnchMergeTree
PARTITION BY p_date
ORDER BY tag_id;

Insert into test_tag_bitmap values (2, [1,2,3,4,5],'2024-01-01'),(5,[3,4,5,6,7,8],'2024-01-01');

select  bitmapCount('5')(tag_id, uids), bitmapCount('2')(tag_id, uids), bitmapCount('2&5')(tag_id, uids), bitmapMultiCount('5','2','2&5')(tag_id, uids) from test_tag_bitmap where p_date = '2024-01-01';

CREATE TABLE test_tag_bitmap_v2 (
f1 Int32, f2 UInt64, f3 String, f4 Int32, uids BitMap64, p_date Date
) engine=CnchMergeTree PARTITION BY p_date ORDER BY f1;

insert into test_tag_bitmap_v2 values (1, 2, 'a', 10, [1,2,3,4,5], '2024-01-01'),(2,2,'b', 11, [3,4,5,6], '2024-01-01'),(3,1,'c', 12, [1,2,4,8],'2024-01-01');

--- new where condition: WHERE ((f3 IN ('a', 'b')) OR (f1 IN (1, 2, 3))) AND (p_date = '2024-01-01')
select
bitmapCount('1')(f1, uids) as r1,
bitmapMultiCount('1|2','2','3')(f1, uids) as r2,
-- bitmapCount('a')(f3, uids) as r3,
-- bitmapCount('a&b')(f3, uids) as r4,
bitmapCount('2')(toInt64(f2), uids) as r5,
bitmapCount('2~1')(toInt64(f2), uids) as r6
from test_tag_bitmap_v2 where p_date='2024-01-01';

-- SELECT bitmapExtract('1')(f1, uids) AS r1, bitmapMultiExtract('1|2', '2', '3')(f1, uids) AS r2, bitmapExtract('a')(f3, uids) AS r3, bitmapExtract('a&b')(f3, uids) AS r4, bitmapExtract('2')(toInt64(f2), uids) AS r5, bitmapExtract('2~1')(toInt64(f2), uids) AS r6 FROM test_tag_bitmap_v2 WHERE p_date='2024-01-01';

-- { echoOn }
explain select bitmapCount('1')(f1, uids) as r1
from test_tag_bitmap_v2 where p_date='2024-01-01'
settings enable_optimizer=1;

explain select bitmapMultiCount('1|2','2','3')(f1, uids) as r2
from test_tag_bitmap_v2 where p_date='2024-01-01'
settings enable_optimizer=1;

explain select
  bitmapCount('1')(f1, uids) as r1,
  bitmapMultiCount('1|2','2','3')(f1, uids) as r2,
  --bitmapCount('a')(f3, uids) as r3,
  --bitmapCount('a&b')(f3, uids) as r4,
  bitmapCount('2')(toInt64(f2), uids) as r5,
  bitmapCount('2~1')(toInt64(f2), uids) as r6,
  bitmapCount('10')(f4, uids) as r7,
  bitmapMultiCount('10|20','20','30')(f4, uids) as r8
from test_tag_bitmap_v2 where p_date='2024-01-01'
settings enable_optimizer=1;

explain select 1
from test_tag_bitmap_v2 where p_date='2024-01-01'
having bitmapCount('1')(f1, uids) > 1
settings enable_optimizer=1;

explain
select 1
union all
select bitmapCount('1')(f1, uids) as r1
from test_tag_bitmap_v2 where p_date='2024-01-01'
settings enable_optimizer=1;

explain
select bitmapCount('1')(x, y) as r1
from (
  select f1 as x, uids as y
  from test_tag_bitmap_v2 where p_date='2024-01-01'
) s
settings enable_optimizer=1;

explain
select bitmapCount('1')(x, y) as r1
from (
  select f1 as x, uids as y
  from test_tag_bitmap_v2 where p_date='2024-01-01'
) s cross join (select 1) as t
settings enable_optimizer=1;

explain
select bitmapCount('1')(f1, uids)
from (select toInt64(1) as f1, arrayToBitmap([1,2,3]) as uids)
settings enable_optimizer=1;

-- { echoOff }

drop table test_tag_bitmap;
drop table test_tag_bitmap_v2;
