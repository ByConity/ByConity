WITH
    toDate('2023-12-07') AS start_date,
    toDate('2023-12-08') AS end_date
SELECT
    arrayJoin(arrayMap(x -> addDays(start_date, x), range(2))) AS first_date,
    arrayJoin(arrayMap(x -> addDays(first_date, x), range(1))) AS event_date
ORDER BY
    first_date ASC,
    event_date ASC;
