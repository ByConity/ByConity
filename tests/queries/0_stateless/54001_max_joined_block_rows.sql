SET max_memory_usage = 1600000;

SET max_joined_block_size_rows = 10000000;
SELECT k1, count(*), sum(n1.number), sum(n2.number) FROM (select number % 20 as k1, number from numbers(10000)) n1 INNER JOIN (select number %20 as k2, number from numbers(10000)) n2 on k1=k2 group by k1; -- { serverError 241 }

SET max_joined_block_size_rows = 1000;
SELECT k1, count(*), sum(n1.number), sum(n2.number) FROM (select number % 20 as k1, number from numbers(10000)) n1 INNER JOIN (select number %20 as k2, number from numbers(10000)) n2 on k1=k2 group by k1;