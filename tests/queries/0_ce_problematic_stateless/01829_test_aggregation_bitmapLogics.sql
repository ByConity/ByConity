DROP TABLE IF EXISTS test.bitmap_agg;
CREATE TABLE test.bitmap_agg (tag Int32, ids BitMap64, p_date Int64) engine=Memory;
insert into test.bitmap_agg values (1, [1,2,3,4,5,6,7,8,9,10], 20211201);
insert into test.bitmap_agg values (2, [6,7,8,9,10,11,12,13,14,15], 20211202);
insert into test.bitmap_agg values (3, [2,4,6,8,10,12], 20211202);

SELECT bitmapColumnOr(arrayToBitmap(emptyArrayUInt32()));
select bitmapColumnOr(ids) from test.bitmap_agg;
select p_date,bitmapColumnOr(ids) from test.bitmap_agg group by p_date order by p_date;

SELECT bitmapColumnAnd(arrayToBitmap(emptyArrayUInt32()));
select bitmapColumnAnd(ids) from test.bitmap_agg;
select p_date,bitmapColumnAnd(ids) from test.bitmap_agg group by p_date order by p_date;

SELECT bitmapColumnXor(arrayToBitmap(emptyArrayUInt32()));
select bitmapColumnXor(ids) from test.bitmap_agg;
select p_date,bitmapColumnXor(ids) from test.bitmap_agg group by p_date order by p_date;

SELECT bitmapColumnHas(ids, 10) from test.bitmap_agg;
SELECT bitmapColumnHas(ids, 0) from test.bitmap_agg;

SELECT bitmapColumnCardinality(arrayToBitmap(emptyArrayUInt32()));
select bitmapColumnCardinality(ids) from test.bitmap_agg;
select p_date,bitmapColumnCardinality(ids) from test.bitmap_agg group by p_date order by p_date;

SELECT bitmapCardinality(bitmapColumnOr(arrayToBitmap(emptyArrayUInt32())));
select bitmapCardinality(bitmapColumnOr(ids)) from test.bitmap_agg;
select p_date,bitmapCardinality(bitmapColumnOr(ids)) from test.bitmap_agg group by p_date order by p_date;

DROP TABLE test.bitmap_agg;