drop DATABASE if exists test_48006;
CREATE DATABASE test_48006;

use test_48006;

set enable_optimizer=1;

SELECT x,arr FROM (SELECT arrayJoin(['Hello', 'Goodbye']) AS x, [1, 2, 3] AS arr) ARRAY JOIN arr;

SELECT x,a FROM (SELECT arrayJoin(['Hello', 'Goodbye']) AS x, [1, 2, 3] AS arr) ARRAY JOIN arr as a;

SELECT arr, element FROM (SELECT [1] AS arr) LEFT ARRAY JOIN arr AS element;

SELECT arr, element FROM (SELECT emptyArrayUInt8() AS arr) LEFT ARRAY JOIN arr AS element;

SELECT arr, element FROM (SELECT arrayJoin([emptyArrayUInt8(), [1], [2, 3]]) AS arr) LEFT ARRAY JOIN arr AS element;

DROP TABLE IF EXISTS array_test;

CREATE TABLE array_test(s String, arr1 Array(UInt8),arr2 Array(String)) ENGINE = CnchMergeTree() order by s;
INSERT INTO array_test VALUES ('Hello', [1,2], ['1','2']), ('World', [3,4,5], ['3','4','5']), ('Goodbye', [], []);

SELECT arr1, arr2, s FROM array_test ARRAY JOIN arr1 order by s;

SELECT arr, arr2, s FROM array_test ARRAY JOIN arr1 as arr order by arr;

SELECT arr1, arr2, s FROM array_test LEFT ARRAY JOIN arr2 order by arr2;

SELECT arr1, arr, s FROM array_test LEFT ARRAY JOIN arr2 as arr order by arr;

SELECT arr1, a, num, mapped, s FROM array_test ARRAY JOIN arr1 AS a, arrayEnumerate(arr2) AS num, arrayMap(x -> x + 1, arr1) AS mapped order by s;

SELECT arr_external, s FROM array_test ARRAY JOIN [1, 2, 3] AS arr_external order by s;

SELECT arr_external, s FROM array_test ARRAY JOIN [null] AS arr_external order by s;

SELECT arr1, arr2, s FROM array_test ARRAY JOIN arr1, arr2 order by s;

SELECT arr1, arr2, s FROM array_test LEFT ARRAY JOIN arr1, arr2 order by s;

SELECT count() FROM array_test ARRAY JOIN arr1, arr2 group by arr1;

SELECT arr, arr2, s FROM array_test ARRAY JOIN arr1 as arr, arr2 where arr < 2 order by s;

SELECT arr1, arr2, s FROM array_test ARRAY JOIN arr1, arr2 where arr1 < 2 order by s;

SELECT arr1, a2, a3 FROM array_test ARRAY JOIN arr1, arr1 as a2, arr1 as a3 order by arr1;

DROP TABLE IF EXISTS array_test;
