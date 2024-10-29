drop table if exists limit_by;

create table limit_by(a Int, b Int, c Int) ENGINE = CnchMergeTree() partition by a order by a;

insert into limit_by values(1, 1, 2), (1, 1, 2), (2, 1, 2), (2, 2, 1), (1, 1, 1), (2, 2, 2);

SELECT t1.a
FROM
(
    SELECT
        a,
        b,
        c
    FROM limit_by
    ORDER BY
        a ASC,
        b ASC
    LIMIT 1 BY c
) AS t1
INNER JOIN
(
    SELECT
        a,
        b,
        c
    FROM limit_by
) AS t2 ON t1.a = t2.a
ORDER BY t1.a ASC;

insert into limit_by values(2, 1, 2), (2, 1, 2), (3, 1, 2), (3, 2, 1), (3, 1, 1), (3, 2, 2);

 SELECT a, b FROM limit_by group by a,b order by a,b LIMIT 1 BY b;

drop table if exists limit_by;
