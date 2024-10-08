SELECT t1.id
FROM
(
    SELECT *
    FROM
    (
        SELECT 1 AS id
    ) AS t1
    INNER JOIN
    (
        SELECT 1 AS id
    ) AS t2 ON t1.id = t2.id
)
INNER JOIN
(
    SELECT 1 AS id
) AS t3 USING (id);

SELECT t1.id
FROM
(
    select * from
    (
        SELECT *
        FROM
        (
            SELECT 1 AS id
        ) AS
        INNER JOIN
        (
            SELECT 1 AS id
        )  using id
    ) as t1
    join
    (
        SELECT *
        FROM
        (
            SELECT 1 AS id
        ) AS
        INNER JOIN
        (
            SELECT 1 AS id
        )  using id
    ) as t2 on t1.id = t2.id
)
INNER JOIN
(
    SELECT 1 AS id
) AS t3 USING (id);

drop table if exists t_12432;
drop table if exists t_12433;
drop table if exists t_12434;

create table t_12432 (id Int32) Engine = CnchMergeTree order by id;
create table t_12433 (id Int32) Engine = CnchMergeTree order by id;
create table t_12434 (id Int32) Engine = CnchMergeTree order by id;

SELECT t1.id
FROM
(
    SELECT *
    FROM
    (
        SELECT id from t_12432
    ) AS t1
    INNER JOIN
    (
        SELECT id from t_12433
    ) AS t2 ON t1.id = t2.id
)
INNER JOIN
(
    SELECT id from t_12434
) AS t3 USING (id);

drop table t_12432;
drop table t_12433;
drop table t_12434;