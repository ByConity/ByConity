
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

DROP TABLE IF EXISTS test.bitmap_test;
CREATE TABLE test.bitmap_test(pickup_date Date, city_id UInt32, uid UInt32)ENGINE = MergeTree order by pickup_date;
INSERT INTO test.bitmap_test SELECT '2019-01-01', 1, number FROM numbers(1,50);
INSERT INTO test.bitmap_test SELECT '2019-01-02', 1, number FROM numbers(11,60);
INSERT INTO test.bitmap_test SELECT '2019-01-03', 2, number FROM numbers(1,10);

select bitmapFromColumn( uid ) as uids from test.bitmap_test;
select pickup_date, bitmapCardinality(bitmapFromColumn(uid)) as user_num, bitmapFromColumn(uid) as users FROM test.bitmap_test group by pickup_date order by pickup_date;

SELECT
    bitmapCardinality(day_today) AS today_users,
    bitmapCardinality(day_before) AS before_users,
    bitmapOrCardinality(day_today, day_before) AS all_users,
    bitmapAndCardinality(day_today, day_before) AS old_users,
    bitmapAndnotCardinality(day_today, day_before) AS new_users,
    bitmapXorCardinality(day_today, day_before) AS diff_users
FROM
(
 SELECT city_id, bitmapFromColumn( uid ) AS day_today FROM test.bitmap_test WHERE pickup_date = '2019-01-02' GROUP BY city_id
) js1
ALL LEFT JOIN
(
 SELECT city_id, bitmapFromColumn( uid ) AS day_before FROM test.bitmap_test WHERE pickup_date = '2019-01-01' GROUP BY city_id
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
 SELECT city_id, bitmapFromColumn( uid ) AS day_today FROM test.bitmap_test WHERE pickup_date = '2019-01-02' GROUP BY city_id
) js1
ALL LEFT JOIN
(
 SELECT city_id, bitmapFromColumn( uid ) AS day_before FROM test.bitmap_test WHERE pickup_date = '2019-01-01' GROUP BY city_id
) js2
USING city_id;

SELECT count(*) FROM test.bitmap_test WHERE bitmapHasAny((SELECT bitmapFromColumn(uid) FROM test.bitmap_test WHERE pickup_date = '2019-01-01'), arrayToBitmap([uid]));

SELECT count(*) FROM test.bitmap_test WHERE bitmapHasAny(arrayToBitmap([uid]), (SELECT bitmapFromColumn(uid) FROM test.bitmap_test WHERE pickup_date = '2019-01-01'));

SELECT count(*) FROM test.bitmap_test WHERE 0 = bitmapHasAny((SELECT bitmapFromColumn(uid) FROM test.bitmap_test WHERE pickup_date = '2019-01-01'), arrayToBitmap([uid]));

SELECT count(*) FROM test.bitmap_test WHERE bitmapContains((SELECT bitmapFromColumn(uid) FROM test.bitmap_test WHERE pickup_date = '2019-01-01'), uid);

SELECT count(*) FROM test.bitmap_test WHERE 0 = bitmapContains((SELECT bitmapFromColumn(uid) FROM test.bitmap_test WHERE pickup_date = '2019-01-01'), uid);

-- PR#8082
SELECT bitmapToArray(bitmapAnd(bitmapFromColumn(uid), arrayToBitmap(CAST([1, 2, 3], 'Array(UInt32)')))) FROM test.bitmap_test GROUP BY city_id;

-- between column and expression test
DROP TABLE IF EXISTS test.bitmap_column_expr_test;
CREATE TABLE test.bitmap_column_expr_test
(
    t DateTime,
    z BitMap64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(t)
ORDER BY t;

INSERT INTO test.bitmap_column_expr_test VALUES (now(), arrayToBitmap(cast([3,19,47] as Array(UInt32))));

SELECT bitmapAndCardinality( arrayToBitmap(cast([19,7] AS Array(UInt32))), z) FROM test.bitmap_column_expr_test;
SELECT bitmapAndCardinality( z, arrayToBitmap(cast([19,7] AS Array(UInt32))) ) FROM test.bitmap_column_expr_test;

SELECT bitmapCardinality(bitmapAnd(arrayToBitmap(cast([19,7] AS Array(UInt32))), z )) FROM test.bitmap_column_expr_test;
SELECT bitmapCardinality(bitmapAnd(z, arrayToBitmap(cast([19,7] AS Array(UInt32))))) FROM test.bitmap_column_expr_test;


DROP TABLE IF EXISTS test.bitmap_column_expr_test2;
CREATE TABLE test.bitmap_column_expr_test2
(
    tag_id String,
    z BitMap64
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO test.bitmap_column_expr_test2 VALUES ('tag1', arrayToBitmap(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO test.bitmap_column_expr_test2 VALUES ('tag2', arrayToBitmap(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO test.bitmap_column_expr_test2 VALUES ('tag3', arrayToBitmap(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT bitmapColumnCardinality(z) FROM test.bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
SELECT arraySort(bitmapToArray(bitmapColumnOr(z))) FROM test.bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');

SELECT bitmapCardinality(bitmapColumnOr(z)) FROM test.bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
SELECT arraySort(bitmapToArray(bitmapColumnOr(z))) FROM test.bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');

SELECT bitmapCardinality(bitmapColumnAnd(z)) FROM test.bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
SELECT arraySort(bitmapToArray(bitmapColumnAnd(z))) FROM test.bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');

SELECT bitmapCardinality(bitmapColumnXor(z)) FROM test.bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
SELECT arraySort(bitmapToArray(bitmapColumnXor(z))) FROM test.bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');

DROP TABLE IF EXISTS test.bitmap_column_expr_test3;
CREATE TABLE test.bitmap_column_expr_test3
(
    tag_id String,
    z BitMap64,
    replace Nested (
        from UInt16,
        to UInt64
    )
)
ENGINE = MergeTree
ORDER BY tag_id;

DROP TABLE IF EXISTS test.numbers10;
CREATE VIEW test.numbers10 AS SELECT number FROM system.numbers LIMIT 10;

INSERT INTO test.bitmap_column_expr_test3(tag_id, z, replace.from, replace.to) SELECT 'tag1', bitmapFromColumn(toUInt64(number)), cast([] as Array(UInt16)), cast([] as Array(UInt64)) FROM test.numbers10;
INSERT INTO test.bitmap_column_expr_test3(tag_id, z, replace.from, replace.to) SELECT 'tag2', bitmapFromColumn(toUInt64(number)), cast([0] as Array(UInt16)), cast([2] as Array(UInt64)) FROM test.numbers10;
INSERT INTO test.bitmap_column_expr_test3(tag_id, z, replace.from, replace.to) SELECT 'tag3', bitmapFromColumn(toUInt64(number)), cast([0,7] as Array(UInt16)), cast([3,101] as Array(UInt64)) FROM test.numbers10;
INSERT INTO test.bitmap_column_expr_test3(tag_id, z, replace.from, replace.to) SELECT 'tag4', bitmapFromColumn(toUInt64(number)), cast([5,999,2] as Array(UInt16)), cast([2,888,20] as Array(UInt64)) FROM test.numbers10;

SELECT tag_id, bitmapToArray(z), replace.from, replace.to, bitmapToArray(bitmapTransform(z, replace.from, replace.to)) FROM test.bitmap_column_expr_test3 ORDER BY tag_id;

DROP TABLE IF EXISTS test.bitmap_test;
DROP TABLE IF EXISTS test.bitmap_column_expr_test;
DROP TABLE IF EXISTS test.bitmap_column_expr_test2;
DROP TABLE IF EXISTS test.numbers10;
DROP TABLE IF EXISTS test.bitmap_column_expr_test3;


-- bitmapHasAny:
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

-- bitmapMin:
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
CREATE TABLE test.bitmap_test(pickup_date Date, city_id UInt32, uid UInt32)ENGINE = Memory;
INSERT INTO test.bitmap_test SELECT '2019-01-01', 1, number FROM numbers(1,50);
INSERT INTO test.bitmap_test SELECT '2019-01-02', 1, number FROM numbers(11,60);
INSERT INTO test.bitmap_test SELECT '2019-01-03', 2, number FROM numbers(1,10);

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
    FROM test.bitmap_test
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
    FROM test.bitmap_test
    WHERE pickup_date = '2019-01-01'
    GROUP BY city_id
) AS js2 USING (city_id) FORMAT Null;
drop table test.bitmap_test;


select emptyBitmap();
select arrayToBitmap(emptyArrayUInt32());
