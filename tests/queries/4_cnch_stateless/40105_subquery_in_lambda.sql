-- { echo }

-- scalar subquery
select length(arrayFilter(x -> x = (select dummy from system.one), range(10)));

-- in subquery
select length(arrayFilter(x -> x in (select dummy from system.one), range(10)));

-- in subquery with max_rows_in_set
select length(arrayFilter(x -> x in (select number from system.numbers limit 10000), range(10))) settings max_rows_in_set = 10; -- { serverError 191 }
select length(arrayFilter(x -> x in (select number % 10 from system.numbers limit 10000), range(10))) settings max_rows_in_set = 10;

-- in subquery, type mismatch
select length(arrayFilter(x -> x in (select toString(dummy) from system.one), range(10)));
-- optimizer fails, as the query is equivalent to `select length(arrayFilter(x -> toString(x) in (1, 1, 1), range(10)))`
-- select length(arrayFilter(x -> toString(x) in (select dummy from system.one), range(10)));
select length(arrayFilter(x -> x in (select '2024-01-01' from system.one), arrayMap(y -> cast(y, 'Date'), ['2024-01-01', '2024-01-02', '2024-01-03'])));
-- select length(arrayFilter(x -> x in (select cast('2024-01-01', 'Date') from system.one), ['2024-01-01', '2024-01-02', '2024-01-03']));

-- in subquery, multiple element
select length(arrayFilter(x -> (x, x) in (select dummy, dummy from system.one), range(10)));
-- select length(arrayFilter(x -> (toString(x), x) in (select dummy, toString(dummy) from system.one), range(10)));
