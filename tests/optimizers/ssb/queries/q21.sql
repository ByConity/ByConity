SELECT
    C_CITY,
    S_CITY,
    toYear(LO_ORDERDATE) AS year,
    sum(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE C_NATION = 'UNITED STATES' AND S_NATION = 'UNITED STATES' AND toYear(LO_ORDERDATE) >= 1992 AND toYear(LO_ORDERDATE) <= 1997
GROUP BY
    C_CITY,
    S_CITY,
    toYear(LO_ORDERDATE)
ORDER BY
    toYear(LO_ORDERDATE) ASC,
    sum(LO_REVENUE) DESC;
