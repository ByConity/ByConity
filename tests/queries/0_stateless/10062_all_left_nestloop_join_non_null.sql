SELECT *
FROM
    (
        SELECT
            intDiv(number, 2) AS k,
            number AS joined
        FROM system.numbers
                 LIMIT 10
    ) AS js1
ALL LEFT JOIN
(
    SELECT number + 3 AS k
    FROM system.numbers
    LIMIT 10
) AS js2 ON js1.k > js2.k settings join_use_nulls=0;