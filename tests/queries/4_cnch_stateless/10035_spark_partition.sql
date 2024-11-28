SELECT '--------------------Check illegal input-----------';
SELECT sparkpartition('abc'); -- { serverError 42 } --
SELECT sparkpartition(111, 400, 1); -- { serverError 42 } --
select sparkpartition(111, '1'); -- { serverError 43 } --

SELECT '--------------------Check const-----------';
SELECT '-- 0-4';
SELECT sparkpartition('abc', 400);
SELECT sparkpartition('111', 400);
SELECT sparkpartition('-111', 400);

SELECT '-- 5-7';
SELECT sparkpartition('abcabc', 400);

SELECT '-- 8-16';
SELECT sparkpartition('abcabcabc', 400);
SELECT sparkpartition('e43970221139c', 400);

SELECT '-- 17-32';
SELECT sparkpartition('abcabcabcabcabcabcabcabc', 400);

SELECT '-- 33-64';
SELECT sparkpartition('abcabcabcabcabcabcabcabcabcabcabcabc', 400);

SELECT '-- 64+';
SELECT sparkpartition('abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc', 400);

SELECT '-- CN';
SELECT sparkpartition('中国人', 400);
SELECT sparkpartition('中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人', 400);

SELECT '-- Int';
SELECT sparkpartition(toInt32(111), 400);
SELECT sparkpartition(toInt32(-111), 400);

SELECT sparkpartition(toInt64(4294967297), 400);
SELECT sparkpartition(toInt64(-4294967297), 400);

SELECT '-- BigInt';
SELECT sparkpartition('340282366920938463463374607431768211457', 400);
SELECT sparkpartition('-340282366920938463463374607431768211457', 400);

SELECT '--------------------Check column-----------';
DROP TABLE IF EXISTS spark_partition_test;
CREATE TABLE spark_partition_test (n Int64, s String) Engine = CnchMergeTree ORDER BY (n);
INSERT INTO spark_partition_test VALUES (1, 'abc'), (2, '111'), (3, '-111'), (4, 'abcabc'), (5, 'abcabcabc'), (6, 'e43970221139c'), (7, 'abcabcabcabcabcabcabcabc'), (8, 'abcabcabcabcabcabcabcabcabcabcabcabc'), (9, 'abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc'), (10, '中国人'), (11, '中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人'), (12, '340282366920938463463374607431768211457'), (13, '-340282366920938463463374607431768211457');
SELECT s, sparkpartition(s, 400) FROM spark_partition_test ORDER BY n;
DROP TABLE spark_partition_test;

CREATE TABLE spark_partition_test (n Int64, v Int32) Engine = CnchMergeTree ORDER BY (n);
INSERT INTO spark_partition_test VALUES (1, 111), (2, -111);
SELECT v, sparkpartition(v, 400) FROM spark_partition_test ORDER BY n;
DROP TABLE spark_partition_test;

CREATE TABLE spark_partition_test (n Int64, v Int64) Engine = CnchMergeTree ORDER BY (n);
INSERT INTO spark_partition_test VALUES (1, 4294967297), (2, -4294967297);
SELECT v, sparkpartition(v, 400) FROM spark_partition_test ORDER BY n;
DROP TABLE spark_partition_test;
