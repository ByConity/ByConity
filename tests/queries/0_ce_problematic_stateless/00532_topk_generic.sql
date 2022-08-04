set enable_push_partial_agg=0; -- result is unstable due to order of input.
SELECT k, topK(v) FROM (SELECT number % 10 AS k, arrayMap(x -> arrayMap(x -> x = 0 ? NULL : toString(x), range(x)), range(intDiv(number, 13))) AS v FROM system.numbers LIMIT 100) GROUP BY k ORDER BY k;
