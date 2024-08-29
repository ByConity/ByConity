SELECT t2.`$phoneid`
FROM
    (SELECT 1 AS k) t1
JOIN
    (SELECT 1 AS k, 'foo' AS `$phoneid`) t2
ON t1.k = t2.k
JOIN (SELECT 1 AS k) t3
ON t1.k = t3.k
SETTINGS check_identifier_begin_valid = 0;

SELECT t2.`$phoneid`
FROM
    (SELECT 1 AS k) t1
JOIN
    (SELECT 1 AS k, 'foo' AS `$phoneid`) t2
ON t1.k = t2.k
SETTINGS check_identifier_begin_valid = 0;
