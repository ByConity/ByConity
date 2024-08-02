create table 53032_mark_set_for_index_optimizer (
    id UInt32, name Nullable(String), dim_data Map(String, String), p_date Date
) ENGINE=CnchMergeTree
order by p_date;

SELECT count()
FROM 53032_mark_set_for_index_optimizer
WHERE ((multiIf(isNull(`name`), '1', dim_data{'a'} LIKE '1%', '1', '1'), p_date) IN [('1', 1982)]);

-- SELECT count()
-- FROM 53032_mark_set_for_index_optimizer
-- WHERE ((multiIf(isNull(`name`), '1', `__dim_data__'a'` LIKE '1%', '1', '1'), p_date) IN [('1', 1982)]);

SELECT count()
FROM 53032_mark_set_for_index_optimizer
WHERE ((multiIf(isNull(name), '1', dim_data{'a'} LIKE '1%', '1', '1'), p_date) IN (
    SELECT
        multiIf(isNull(name), '1', dim_data{'a'} LIKE '1%', '1', '1'),
        p_date
    FROM
    (
        SELECT
            name,
            dim_data,
            p_date
        FROM 53032_mark_set_for_index_optimizer
        WHERE p_date = '2024-06-05'
        LIMIT 1
    )
))
LIMIT 1;
