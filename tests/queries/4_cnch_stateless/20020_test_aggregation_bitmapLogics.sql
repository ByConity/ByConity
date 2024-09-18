DROP TABLE IF EXISTS bitmap_agg_20020;
CREATE TABLE IF NOT EXISTS bitmap_agg_20020 (tag Int32, ids BitMap64, p_date Int64) engine=CnchMergeTree ORDER BY (tag, p_date);
insert into bitmap_agg_20020 values (1, [1,2,3,4,5,6,7,8,9,10], 20211201);
insert into bitmap_agg_20020 values (2, [6,7,8,9,10,11,12,13,14,15], 20211202);
insert into bitmap_agg_20020 values (3, [2,4,6,8,10,12], 20211202);

SELECT bitmapColumnOr(arrayToBitmap(emptyArrayUInt32()));
select bitmapColumnOr(ids) from bitmap_agg_20020;
select p_date,bitmapColumnOr(ids) from bitmap_agg_20020 group by p_date order by p_date;

SELECT bitmapColumnAnd(arrayToBitmap(emptyArrayUInt32()));
select bitmapColumnAnd(ids) from bitmap_agg_20020;
select p_date,bitmapColumnAnd(ids) from bitmap_agg_20020 group by p_date order by p_date;

SELECT bitmapColumnXor(arrayToBitmap(emptyArrayUInt32()));
select bitmapColumnXor(ids) from bitmap_agg_20020;
select p_date,bitmapColumnXor(ids) from bitmap_agg_20020 group by p_date order by p_date;

SELECT bitmapColumnHas(ids, 10) from bitmap_agg_20020;
SELECT bitmapColumnHas(ids, 0) from bitmap_agg_20020;

SELECT bitmapColumnCardinality(arrayToBitmap(emptyArrayUInt32()));
select bitmapColumnCardinality(ids) from bitmap_agg_20020;
select p_date,bitmapColumnCardinality(ids) from bitmap_agg_20020 group by p_date order by p_date;

SELECT bitmapCardinality(bitmapColumnOr(arrayToBitmap(emptyArrayUInt32())));
select bitmapCardinality(bitmapColumnOr(ids)) from bitmap_agg_20020;
select p_date,bitmapCardinality(bitmapColumnOr(ids)) from bitmap_agg_20020 group by p_date order by p_date;

DROP TABLE IF EXISTS bitmap_agg_20020;