SELECT 1 FROM (SELECT groupArray(total_gmv) AS arr_val FROM (SELECT cast(1 AS Nullable(Int64)) as total_gmv)) array JOIN arr_val;

-- TODO
-- SELECT 1 FROM (SELECT groupUniqArray(upper(query)) AS queryArrayUpper FROM (select cast('1' as Nullable(String)) as query)) AS t WHERE has(queryArrayUpper, upper('1')) = 1;

SELECT pt, arr_val, row_number FROM (SELECT pt, groupArray(total_gmv) AS arr_val, arrayEnumerate(arr_val) AS row_number FROM (SELECT 1 as pt, cast(1 as Nullable(Int32)) as total_gmv) GROUP BY pt) array JOIN arr_val, row_number where row_number < 11;

SELECT pt, arr_val, row_number FROM (SELECT pt, groupArray(total_gmv) AS arr_val, arrayEnumerate(arr_val) AS row_number FROM (SELECT 1 as pt, cast(1 as Nullable(Int32)) as total_gmv) GROUP BY pt) array JOIN arr_val, row_number;
