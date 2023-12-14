SET max_memory_usage = '100M';
SELECT cityHash64(rand() % 1000) as n, groupBitmapState(number) FROM numbers_mt(2000000000) GROUP BY n; -- { serverError 241 }

SELECT cityHash64(rand() % 1000) as n, bitmapCardinality(bitmapFromColumn(number)) FROM numbers_mt(2000000000) GROUP BY n; -- { serverError 241 }

SET max_memory_usage = '1000M';
SELECT cityHash64(rand() % 1000) as n, groupBitmapState(number) FROM numbers_mt(2000000000) GROUP BY n; -- { serverError 241 }

SELECT cityHash64(rand() % 1000) as n, bitmapCardinality(bitmapFromColumn(number)) FROM numbers_mt(2000000000) GROUP BY n; -- { serverError 241 }