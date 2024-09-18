
-----------------------
---- BitMap64 Type ----
-----------------------

SELECT 'BitMap64 Type';

SELECT arrayToBitmap([1, 2, 3, 4, 5]) as bitmap, toTypeName(bitmap);
SELECT arrayToBitmap(emptyArrayUInt32()) as bitmap, toTypeName(bitmap);

SELECT bitmapToArray(arrayToBitmap([1, 2, 3, 4, 5]));
SELECT bitmapAnd(arrayToBitmap([1,2,3]),arrayToBitmap([3,4,5]));
SELECT bitmapOr(arrayToBitmap([1,2,3]),arrayToBitmap([3,4,5]));
SELECT bitmapXor(arrayToBitmap([1,2,3]),arrayToBitmap([3,4,5]));
SELECT bitmapAndnot(arrayToBitmap([1,2,3]),arrayToBitmap([3,4,5]));
SELECT bitmapCardinality(arrayToBitmap([1, 2, 3, 4, 5]));
SELECT bitmapAndCardinality(arrayToBitmap([1,2,3]),arrayToBitmap([3,4,5]));
SELECT bitmapOrCardinality(arrayToBitmap([1,2,3]),arrayToBitmap([3,4,5]));
SELECT bitmapXorCardinality(arrayToBitmap([1,2,3]),arrayToBitmap([3,4,5]));
SELECT bitmapAndnotCardinality(arrayToBitmap([1,2,3]),arrayToBitmap([3,4,5]));
SELECT bitmapAndCardinality(arrayToBitmap([100, 200, 500]), arrayToBitmap(CAST([100, 200], 'Array(UInt16)')));
SELECT bitmapToArray(bitmapAnd(arrayToBitmap([100, 200, 500]), arrayToBitmap(CAST([100, 200], 'Array(UInt16)'))));

DROP TABLE IF EXISTS bitmap_function_test_20020;
CREATE TABLE IF NOT EXISTS bitmap_function_test_20020(pickup_date Date, city_id UInt32, uid UInt32)ENGINE = CnchMergeTree order by pickup_date;
INSERT INTO bitmap_function_test_20020 SELECT '2019-01-01', 1, number FROM numbers(1,50);
INSERT INTO bitmap_function_test_20020 SELECT '2019-01-02', 1, number FROM numbers(11,60);
INSERT INTO bitmap_function_test_20020 SELECT '2019-01-03', 2, number FROM numbers(1,10);

select bitmapFromColumn( uid ) as uids from bitmap_function_test_20020;
select pickup_date, bitmapCardinality(bitmapFromColumn(uid)) as user_num, bitmapFromColumn(uid) as users FROM bitmap_function_test_20020 group by pickup_date order by pickup_date;

SELECT
    bitmapCardinality(day_today) AS today_users,
    bitmapCardinality(day_before) AS before_users,
    bitmapOrCardinality(day_today, day_before) AS all_users,
    bitmapAndCardinality(day_today, day_before) AS old_users,
    bitmapAndnotCardinality(day_today, day_before) AS new_users,
    bitmapXorCardinality(day_today, day_before) AS diff_users
FROM
(
 SELECT city_id, bitmapFromColumn( uid ) AS day_today FROM bitmap_function_test_20020 WHERE pickup_date = '2019-01-02' GROUP BY city_id
) js1
ALL LEFT JOIN
(
 SELECT city_id, bitmapFromColumn( uid ) AS day_before FROM bitmap_function_test_20020 WHERE pickup_date = '2019-01-01' GROUP BY city_id
) js2
USING city_id;

SELECT
    bitmapCardinality(day_today) AS today_users,
    bitmapCardinality(day_before) AS before_users,
    bitmapCardinality(bitmapOr(day_today, day_before))ll_users,
    bitmapCardinality(bitmapAnd(day_today, day_before)) AS old_users,
    bitmapCardinality(bitmapAndnot(day_today, day_before)) AS new_users,
    bitmapCardinality(bitmapXor(day_today, day_before)) AS diff_users
FROM
(
 SELECT city_id, bitmapFromColumn( uid ) AS day_today FROM bitmap_function_test_20020 WHERE pickup_date = '2019-01-02' GROUP BY city_id
) js1
ALL LEFT JOIN
(
 SELECT city_id, bitmapFromColumn( uid ) AS day_before FROM bitmap_function_test_20020 WHERE pickup_date = '2019-01-01' GROUP BY city_id
) js2
USING city_id;

SELECT count(*) FROM bitmap_function_test_20020 WHERE bitmapHasAny((SELECT bitmapFromColumn(uid) FROM bitmap_function_test_20020 WHERE pickup_date = '2019-01-01'), arrayToBitmap([uid])) SETTINGS enable_optimizer=1;

SELECT count(*) FROM bitmap_function_test_20020 WHERE bitmapHasAny(arrayToBitmap([uid]), (SELECT bitmapFromColumn(uid) FROM bitmap_function_test_20020 WHERE pickup_date = '2019-01-01')) SETTINGS enable_optimizer=1;

SELECT count(*) FROM bitmap_function_test_20020 WHERE 0 = bitmapHasAny((SELECT bitmapFromColumn(uid) FROM bitmap_function_test_20020 WHERE pickup_date = '2019-01-01'), arrayToBitmap([uid])) SETTINGS enable_optimizer=1;

SELECT count(*) FROM bitmap_function_test_20020 WHERE bitmapContains((SELECT bitmapFromColumn(uid) FROM bitmap_function_test_20020 WHERE pickup_date = '2019-01-01'), uid) SETTINGS enable_optimizer=1;

SELECT count(*) FROM bitmap_function_test_20020 WHERE 0 = bitmapContains((SELECT bitmapFromColumn(uid) FROM bitmap_function_test_20020 WHERE pickup_date = '2019-01-01'), uid) SETTINGS enable_optimizer=1;

-- PR#8082
SELECT bitmapToArray(bitmapAnd(bitmapFromColumn(uid), arrayToBitmap(CAST([1, 2, 3], 'Array(UInt32)')))) FROM bitmap_function_test_20020 GROUP BY city_id ORDER BY city_id;

-- between column and expression test
DROP TABLE IF EXISTS bitmap_column_expr_test_20020;
CREATE TABLE IF NOT EXISTS bitmap_column_expr_test_20020
(
    t DateTime,
    z BitMap64
)
ENGINE = CnchMergeTree
PARTITION BY toYYYYMMDD(t)
ORDER BY t;

INSERT INTO bitmap_column_expr_test_20020 VALUES (now(), arrayToBitmap(cast([3,19,47] as Array(UInt32))));

SELECT bitmapAndCardinality( arrayToBitmap(cast([19,7] AS Array(UInt32))), z) FROM bitmap_column_expr_test_20020;
SELECT bitmapAndCardinality( z, arrayToBitmap(cast([19,7] AS Array(UInt32))) ) FROM bitmap_column_expr_test_20020;

SELECT bitmapCardinality(bitmapAnd(arrayToBitmap(cast([19,7] AS Array(UInt32))), z )) FROM bitmap_column_expr_test_20020;
SELECT bitmapCardinality(bitmapAnd(z, arrayToBitmap(cast([19,7] AS Array(UInt32))))) FROM bitmap_column_expr_test_20020;


DROP TABLE IF EXISTS bitmap_column_expr_test2_20020;
CREATE TABLE IF NOT EXISTS bitmap_column_expr_test2_20020
(
    tag_id String,
    z BitMap64
)
ENGINE = CnchMergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2_20020 VALUES ('tag1', arrayToBitmap(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2_20020 VALUES ('tag2', arrayToBitmap(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2_20020 VALUES ('tag3', arrayToBitmap(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT bitmapColumnCardinality(z) FROM bitmap_column_expr_test2_20020 WHERE like(tag_id, 'tag%');
SELECT arraySort(bitmapToArray(bitmapColumnOr(z))) FROM bitmap_column_expr_test2_20020 WHERE like(tag_id, 'tag%');

SELECT bitmapCardinality(bitmapColumnOr(z)) FROM bitmap_column_expr_test2_20020 WHERE like(tag_id, 'tag%');
SELECT arraySort(bitmapToArray(bitmapColumnOr(z))) FROM bitmap_column_expr_test2_20020 WHERE like(tag_id, 'tag%');

SELECT bitmapCardinality(bitmapColumnAnd(z)) FROM bitmap_column_expr_test2_20020 WHERE like(tag_id, 'tag%');
SELECT arraySort(bitmapToArray(bitmapColumnAnd(z))) FROM bitmap_column_expr_test2_20020 WHERE like(tag_id, 'tag%');

SELECT bitmapCardinality(bitmapColumnXor(z)) FROM bitmap_column_expr_test2_20020 WHERE like(tag_id, 'tag%');
SELECT arraySort(bitmapToArray(bitmapColumnXor(z))) FROM bitmap_column_expr_test2_20020 WHERE like(tag_id, 'tag%');

-- select '=== bitmapTransform ===';
DROP TABLE IF EXISTS bitmap_column_expr_test3_20020;
CREATE TABLE IF NOT EXISTS bitmap_column_expr_test3_20020
(
    tag_id String,
    z BitMap64,
    replace Nested (
        from UInt16,
        to UInt64
    )
)
ENGINE = CnchMergeTree
ORDER BY tag_id;

DROP TABLE IF EXISTS numbers10;
CREATE VIEW numbers10 AS SELECT number FROM system.numbers LIMIT 10;

INSERT INTO bitmap_column_expr_test3_20020(tag_id, z, replace.from, replace.to) SELECT 'tag1', bitmapFromColumn(toUInt64(number)), cast([] as Array(UInt16)), cast([] as Array(UInt64)) FROM numbers10;
INSERT INTO bitmap_column_expr_test3_20020(tag_id, z, replace.from, replace.to) SELECT 'tag2', bitmapFromColumn(toUInt64(number)), cast([0] as Array(UInt16)), cast([2] as Array(UInt64)) FROM numbers10;
INSERT INTO bitmap_column_expr_test3_20020(tag_id, z, replace.from, replace.to) SELECT 'tag3', bitmapFromColumn(toUInt64(number)), cast([0,7] as Array(UInt16)), cast([3,101] as Array(UInt64)) FROM numbers10;
INSERT INTO bitmap_column_expr_test3_20020(tag_id, z, replace.from, replace.to) SELECT 'tag4', bitmapFromColumn(toUInt64(number)), cast([5,999,2] as Array(UInt16)), cast([2,888,20] as Array(UInt64)) FROM numbers10;

SELECT tag_id, bitmapToArray(z), replace.from, replace.to, bitmapToArray(bitmapTransform(z, replace.from, replace.to)) FROM bitmap_column_expr_test3_20020 ORDER BY tag_id;

DROP TABLE IF EXISTS bitmap_function_test_20020;
DROP TABLE IF EXISTS bitmap_column_expr_test_20020;
DROP TABLE IF EXISTS bitmap_column_expr_test2_20020;
DROP TABLE IF EXISTS numbers10;
DROP TABLE IF EXISTS bitmap_column_expr_test3_20020;


-- bitmapHasAny:
select '=== bitmapHasAny ===';
---- Empty
SELECT bitmapHasAny(arrayToBitmap([1, 2, 3, 5]), arrayToBitmap(emptyArrayUInt8()));
SELECT bitmapHasAny(arrayToBitmap(emptyArrayUInt32()), arrayToBitmap(emptyArrayUInt32()));
SELECT bitmapHasAny(arrayToBitmap(emptyArrayUInt16()), arrayToBitmap([1, 2, 3, 500]));
---- Small x Small
SELECT bitmapHasAny(arrayToBitmap([1, 2, 3, 5]),arrayToBitmap([0, 3, 7]));
SELECT bitmapHasAny(arrayToBitmap([1, 2, 3, 5]),arrayToBitmap([0, 4, 7]));
---- Small x Large
select bitmapHasAny(arrayToBitmap([100,110,120]),arrayToBitmap([ 99, 100, 101,
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33]));
select bitmapHasAny(arrayToBitmap([100,200,500]),arrayToBitmap([ 99, 101, 600,
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33]));
---- Large x Small
select bitmapHasAny(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,230]),arrayToBitmap([ 99, 100, 101]));
select bitmapHasAny(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]),arrayToBitmap([ 99, 101, 600]));
---- Large x Large
select bitmapHasAny(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    40,50,60]),arrayToBitmap([ 41, 50, 61,
    99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,66,65]));
select bitmapHasAny(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    40,50,60]),arrayToBitmap([ 41, 49, 51, 61,
    99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,66,65]));

-- bitmapHasAll:
select '=== bitmapHasAll ===';
---- Empty
SELECT bitmapHasAll(arrayToBitmap([1, 2, 3, 5]), arrayToBitmap(emptyArrayUInt8()));
SELECT bitmapHasAll(arrayToBitmap(emptyArrayUInt32()), arrayToBitmap(emptyArrayUInt32()));
SELECT bitmapHasAll(arrayToBitmap(emptyArrayUInt16()), arrayToBitmap([1, 2, 3, 500]));
---- Small x Small
select bitmapHasAll(arrayToBitmap([1,5,7,9]),arrayToBitmap([5,7]));
select bitmapHasAll(arrayToBitmap([1,5,7,9]),arrayToBitmap([5,7,2]));
---- Small x Large
select bitmapHasAll(arrayToBitmap([100,110,120]),arrayToBitmap([ 99, 100, 101,
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33]));
select bitmapHasAll(arrayToBitmap([100,200,500]),arrayToBitmap([ 99, 101, 600,
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33]));
---- Small x LargeSmall
select bitmapHasAll(arrayToBitmap([1,5,7,9]),bitmapXor(arrayToBitmap([1,5,7]), arrayToBitmap([5,7,9])));
select bitmapHasAll(arrayToBitmap([1,5,7,9]),bitmapXor(arrayToBitmap([1,5,7]), arrayToBitmap([2,5,7])));
---- Large x Small
select bitmapHasAll(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]),arrayToBitmap([100, 500]));
select bitmapHasAll(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]),arrayToBitmap([ 99, 100, 500]));
---- LargeSmall x Small
select bitmapHasAll(bitmapXor(arrayToBitmap([1,7]), arrayToBitmap([5,7,9])), arrayToBitmap([1,5]));
select bitmapHasAll(bitmapXor(arrayToBitmap([1,7]), arrayToBitmap([5,7,9])), arrayToBitmap([1,5,7]));
---- Large x Large
select bitmapHasAll(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]),arrayToBitmap([ 100, 200, 500,
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33]));
select bitmapHasAll(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]),arrayToBitmap([ 100, 200, 501,
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33]));

-- bitmapContains:
select '=== bitmapContains ===';
---- Empty
SELECT bitmapContains(arrayToBitmap(emptyArrayUInt32()), toUInt32(0));
SELECT bitmapContains(arrayToBitmap(emptyArrayUInt16()), toUInt32(5));
---- Small
select bitmapContains(arrayToBitmap([1,5,7,9]),toUInt32(0));
select bitmapContains(arrayToBitmap([1,5,7,9]),toUInt32(9));
---- Large
select bitmapContains(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]),toUInt32(100));
select bitmapContains(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]),toUInt32(101));
select bitmapContains(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]),toUInt32(500));

-- bitmapSubsetInRange:
select '=== bitmapSubsetInRange ===';
---- Empty
SELECT bitmapToArray(bitmapSubsetInRange(arrayToBitmap(emptyArrayUInt32()), 0, 10));
SELECT bitmapToArray(bitmapSubsetInRange(arrayToBitmap(emptyArrayUInt16()), 0, 10));
---- Small
select bitmapToArray(bitmapSubsetInRange(arrayToBitmap([1,5,7,9]), 0, 4));
select bitmapToArray(bitmapSubsetInRange(arrayToBitmap([1,5,7,9]), 10, 10));
select bitmapToArray(bitmapSubsetInRange(arrayToBitmap([1,5,7,9]), 3, 7));
---- Large
select bitmapToArray(bitmapSubsetInRange(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]), toUInt32(0), toUInt32(100)));
select bitmapToArray(bitmapSubsetInRange(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]), toUInt32(30), toUInt32(200)));
select bitmapToArray(bitmapSubsetInRange(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]), toUInt32(100), toUInt32(200)));

-- bitmapSubsetLimit:
select '=== bitmapSubsetLimit ===';
---- Empty
SELECT bitmapToArray(bitmapSubsetLimit(arrayToBitmap(emptyArrayUInt32()), toUInt32(0), toUInt32(10)));
SELECT bitmapToArray(bitmapSubsetLimit(arrayToBitmap(emptyArrayUInt16()), toUInt32(0), toUInt32(10)));
---- Small
select bitmapToArray(bitmapSubsetLimit(arrayToBitmap([1,5,7,9]), toUInt32(0), toUInt32(4)));
select bitmapToArray(bitmapSubsetLimit(arrayToBitmap([1,5,7,9]), toUInt32(10), toUInt32(10)));
select bitmapToArray(bitmapSubsetLimit(arrayToBitmap([1,5,7,9]), toUInt32(3), toUInt32(7)));
---- Large
select bitmapToArray(bitmapSubsetLimit(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]), toUInt32(0), toUInt32(100)));
select bitmapToArray(bitmapSubsetLimit(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]), toUInt32(30), toUInt32(200)));
select bitmapToArray(bitmapSubsetLimit(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]), toUInt32(100), toUInt32(200)));

--subBitmap:
select '=== subBitmap ===';
-- Empty
SELECT bitmapToArray(subBitmap(arrayToBitmap(emptyArrayUInt32()), toUInt8(0), toUInt32(10)));
SELECT bitmapToArray(subBitmap(arrayToBitmap(emptyArrayUInt16()), toUInt32(0), toUInt64(10)));
-- Small
SELECT bitmapToArray(subBitmap(arrayToBitmap([1,5,7,9]), toUInt8(0), toUInt32(4)));
SELECT bitmapToArray(subBitmap(arrayToBitmap([1,5,7,9]), toUInt32(1), toUInt64(4)));
SELECT bitmapToArray(subBitmap(arrayToBitmap([1,5,7,9]), toUInt16(1), toUInt32(2)));
-- Large
SELECT bitmapToArray(subBitmap(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]), toUInt32(0), toUInt32(10)));
SELECT bitmapToArray(subBitmap(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]), toUInt32(30), toUInt32(200)));
SELECT bitmapToArray(subBitmap(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]), toUInt32(34), toUInt16(3)));

-- bitmapMin:
select '=== bitmapMin ===';
---- Empty
SELECT bitmapMin(arrayToBitmap(emptyArrayUInt8()));
SELECT bitmapMin(arrayToBitmap(emptyArrayUInt16()));
SELECT bitmapMin(arrayToBitmap(emptyArrayUInt32()));
---- Small
select bitmapMin(arrayToBitmap([1,5,7,9]));
---- Large
select bitmapMin(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]));

-- bitmapMax:
select '=== bitmapMax ===';
---- Empty
SELECT bitmapMax(arrayToBitmap(emptyArrayUInt8()));
SELECT bitmapMax(arrayToBitmap(emptyArrayUInt16()));
SELECT bitmapMax(arrayToBitmap(emptyArrayUInt32()));
---- Small
select bitmapMax(arrayToBitmap([1,5,7,9]));
---- Large
select bitmapMax(arrayToBitmap([
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
    100,200,500]));

-- reproduce #18911
CREATE TABLE IF  NOT EXISTS bitmap_function_test_20020(pickup_date Date, city_id UInt32, uid UInt32)ENGINE = CnchMergeTree order by city_id;
INSERT INTO bitmap_function_test_20020 SELECT '2019-01-01', 1, number FROM numbers(1,50);
INSERT INTO bitmap_function_test_20020 SELECT '2019-01-02', 1, number FROM numbers(11,60);
INSERT INTO bitmap_function_test_20020 SELECT '2019-01-03', 2, number FROM numbers(1,10);

SELECT
    bitmapCardinality(day_today) AS today_users,
    bitmapCardinality(day_before) AS before_users,
    bitmapOrCardinality(day_today, day_before) AS all_users,
    bitmapAndCardinality(day_today, day_before) AS old_users,
    bitmapAndnotCardinality(day_today, day_before) AS new_users,
    bitmapXorCardinality(day_today, day_before) AS diff_users
FROM
(
    SELECT
        city_id,
        bitmapFromColumn(uid) AS day_today
    FROM bitmap_function_test_20020
    WHERE pickup_date = '2019-01-02'
    GROUP BY
        rand((rand((rand('') % nan) = NULL) % 7) % rand(NULL)),
        city_id
) AS js1
ALL LEFT JOIN
(
    SELECT
        city_id,
        bitmapFromColumn(uid) AS day_before
    FROM bitmap_function_test_20020
    WHERE pickup_date = '2019-01-01'
    GROUP BY city_id
) AS js2 USING (city_id) FORMAT Null;

drop table if exists bitmap_function_test_20020;


select emptyBitmap();
select arrayToBitmap(emptyArrayUInt32());
