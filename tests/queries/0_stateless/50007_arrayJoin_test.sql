SELECT arrayJoin(toNullable(array(1, 2, 3)));
SELECT arrayJoin(toNullable(array(NULL)));
SELECT arrayJoin(toNullable(1)); -- { serverError 53 }
SELECT arrayJoin(toNullable(array(tuple(1, 'a'), tuple(2, 'b'))));
SELECT 1 FROM (SELECT groupArray(total_gmv) AS arr_val FROM (SELECT cast(1 AS Nullable(Int64)) as total_gmv)) array JOIN arr_val;
SELECT pt, arr_val, row_number FROM (SELECT pt, groupArray(total_gmv) AS arr_val, arrayEnumerate(arr_val) AS row_number FROM (SELECT 1 as pt, cast(1 as Nullable(Int32)) as total_gmv) GROUP BY pt) array JOIN arr_val, row_number where row_number < 11;
SELECT pt, arr_val, row_number FROM (SELECT pt, groupArray(total_gmv) AS arr_val, arrayEnumerate(arr_val) AS row_number FROM (SELECT 1 as pt, cast(1 as Nullable(Int32)) as total_gmv) GROUP BY pt) array JOIN arr_val, row_number;
