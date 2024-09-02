WITH r AS
    (
        SELECT 1 AS a
    )
SELECT a
FROM r
WHERE 1 = 0
UNION ALL
SELECT a
FROM r
WHERE a = 1
SETTINGS cte_mode = 'SHARED';
