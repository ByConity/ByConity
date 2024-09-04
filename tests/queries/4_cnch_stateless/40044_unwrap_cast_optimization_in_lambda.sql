SET enable_optimizer=1;

SELECT 0 WHERE notEmpty(arrayFilter(x -> (toInt64(x) > 1), [1, 2]));
SELECT 0 WHERE notEmpty(arrayFilter(x -> notEmpty(arrayFilter(y -> (toInt64(y) > 1 AND toInt64(y) > length(x)), x)), [[1, 2], [3]]));

EXPLAIN SELECT 0 WHERE notEmpty(arrayFilter(x -> (toInt64(x) > 1), [1, 2]));
EXPLAIN SELECT 0 WHERE notEmpty(arrayFilter(x -> notEmpty(arrayFilter(y -> (toInt64(y) > 1 AND toInt64(y) > length(x)), x)), [[1, 2], [3]]));
