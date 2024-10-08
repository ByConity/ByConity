DROP TABLE if exists test_stack;

set parse_literal_as_decimal=0;

CREATE TABLE test_stack(p_date Date, x Int32) Engine = CnchMergeTree PARTITION BY p_date ORDER BY x;
INSERT INTO test_stack VALUES ('2019-11-01', 1), ('2019-11-01', 3), ('2019-10-01', 6);
INSERT INTO test_stack VALUES ('2019-11-01', 6), ('2019-11-01', 4), ('2019-10-01', 1);
SELECT countStack(1, 10, 1)(1, x) FROM test_stack;
SELECT sumStack(1, 10, 1)(1, x) FROM test_stack;
SELECT uniqExactStack(1, 10, 1)(x, x) FROM test_stack;
SELECT quantileExactStack(1, 10, 1)(x, x) FROM test_stack;
SELECT quantileExactStack(0.1, 1, 10, 1)(x, x) FROM test_stack;
SELECT quantileExactStack(0.5, 1, 10, 1)(x, x) FROM test_stack;

DROP TABLE test_stack;

SELECT MergeStreamStack(a) FROM (SELECT CAST([(1, 2), (2, 2)], 'Array(Tuple(Int8, Int8))') AS a union all SELECT CAST([(1, 2), (2, 4)], 'Array(Tuple(Int8, Int8))') AS a);
SELECT MergeStreamStack(a) FROM (SELECT CAST([(1, 2), (2, 2)], 'Array(Tuple(Int32, Int32))') AS a union all SELECT CAST([(1, 2), (2, 4)], 'Array(Tuple(Int32, Int32))') AS a);
SELECT MergeStreamStack(a) FROM (SELECT CAST([(1, 2), (2, 2)], 'Array(Tuple(Int64, Int64))') AS a union all SELECT CAST([(1, 2), (2, 4)], 'Array(Tuple(Int64, Int64))') AS a);
SELECT MergeStreamStack(a) FROM (SELECT CAST([(1, 2), (2, 2)], 'Array(Tuple(Int8, Int64))') AS a union all SELECT CAST([(1, 2), (2, 4)], 'Array(Tuple(Int8, Int64))') AS a);
SELECT MergeStreamStack(a) FROM (SELECT CAST([(1, 2), (2, 2)], 'Array(Tuple(Int8, Float32))') AS a union all SELECT CAST([(1, 2), (2, 4)], 'Array(Tuple(Int8, Float32))') AS a);
SELECT MergeStreamStack(a) FROM (SELECT CAST([(1, 2), (2, 2)], 'Array(Tuple(Int64, Float64))') AS a union all SELECT CAST([(1, 2), (2, 4)], 'Array(Tuple(Int64, Float64))') AS a);
SELECT MergeStreamStack(a) FROM (SELECT CAST([(1, 2), (2, 2)], 'Array(Tuple(Int32, Int32))') AS a union all SELECT CAST([(1, 2), (3, 4)], 'Array(Tuple(Int32, Int32))') AS a); -- { serverError 36 }
SELECT MergeStreamStack(a) FROM (SELECT CAST([(1, 2), (2, 2)], 'Array(Tuple(Int32, Int32))') AS a union all SELECT CAST([(1, 2), (2, 4), (3, 4)], 'Array(Tuple(Int32, Int32))') AS a); -- { serverError 190 }
