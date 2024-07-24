SET max_memory_usage = 1600000;

select 'test1';

SET max_joined_block_size_rows = 10000000;
SELECT k1, count(*), sum(n1.number), sum(n2.number) FROM (select number % 20 as k1, number from numbers(10000)) n1 INNER JOIN (select number %20 as k2, number from numbers(10000)) n2 on k1=k2 group by k1; -- { serverError 241 }

select 'test2';

SET max_joined_block_size_rows = 1000;
SELECT k1, count(*), sum(n1.number), sum(n2.number) FROM (select number % 20 as k1, number from numbers(10000)) n1 INNER JOIN (select number %20 as k2, number from numbers(10000)) n2 on k1=k2 group by k1;

select 'test_grace_hash_inner_join';
SET join_use_nulls = 0;
SET max_threads = 4;
SET max_memory_usage = 32000000;
SET join_algorithm = 'grace_hash';
SET max_joined_block_size_rows = 1000;
SET grace_hash_join_initial_buckets = 4;
SELECT k1, count(*), sum(n1.number), sum(n2.number) FROM (select number % 20 as k1, number from numbers(10000)) n1 INNER JOIN (select number %20 as k2, number from numbers(10000)) n2 on k1=k2 group by k1;


select 'test_grace_hash_left_join';
SELECT k1,k2, count(*), sum(n1.number), sum(n2.number) FROM (select (number % 20)*2 as k1, number from numbers(10000)) n1 LEFT JOIN (select (number %20)*3 as k2, number from numbers(10000)) n2 on k1=k2 group by k1,k2 order by k1,k2;


select 'test_grace_hash_right_join';
SELECT k1,k2, count(*), sum(n1.number), sum(n2.number) FROM (select (number % 20)*2 as k1, number from numbers(10000)) n1 RIGHT JOIN (select (number %20)*3 as k2, number from numbers(10000)) n2 on k1=k2 group by k1,k2 order by k1,k2;


select 'test_grace_hash_full_join';
SELECT k1,k2, count(*), sum(n1.number), sum(n2.number) FROM (select (number % 20)*2 as k1, number from numbers(10000)) n1 FULL JOIN (select (number %20)*3 as k2, number from numbers(10000)) n2 on k1=k2 group by k1,k2 order by k1,k2;

