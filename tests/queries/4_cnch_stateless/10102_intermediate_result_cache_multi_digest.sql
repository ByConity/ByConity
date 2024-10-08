set enable_optimizer=1;
set wait_intermediate_result_cache=0;
set enable_intermediate_result_cache=1;

DROP TABLE if exists multi_digest_all;

CREATE TABLE multi_digest_all(c1 UInt64, c2 UInt64) ENGINE = CnchMergeTree PARTITION BY c1 ORDER BY c1;

insert into multi_digest_all select 1, 1 from numbers(100);
insert into multi_digest_all select 2, 2 from numbers(100);
insert into multi_digest_all select 3, 3 from numbers(100);

SELECT /*multi_digest_a*/c1, sum_c2, b.sum_c2 FROM
(
    SELECT * FROM
    (
        SELECT c1, sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 1) AND (c1 <= 2) GROUP BY c1
    )
    ANY LEFT JOIN
    (
        SELECT c1, sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 2) AND (c1 <= 3) GROUP BY c1
    ) AS b USING (c1)
) ORDER BY c1;

SELECT /*multi_digest_b*/c1, sum_c2, b.sum_c2 FROM
(
    SELECT * FROM
    (
        SELECT c1, sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 1) AND (c1 <= 2) GROUP BY c1
    )
    ANY LEFT JOIN
    (
        SELECT c1, sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 2) AND (c1 <= 3) GROUP BY c1
    ) AS b USING (c1)
) ORDER BY c1 settings max_bytes_to_read = 1;

SELECT /*multi_digest_c*/* FROM
(
    SELECT * FROM
    (
        SELECT c1, sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 1) AND (c1 <= 2) GROUP BY c1
    )
    UNION ALL
    (
        SELECT c1, sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 2) AND (c1 <= 3) GROUP BY c1
    )
) ORDER BY c1, sum_c2 SETTINGS enable_sharding_optimize = 1;

SELECT /*multi_digest_d*/* FROM
(
    SELECT * FROM
    (
        SELECT c1, sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 1) AND (c1 <= 2) GROUP BY c1
    )
    UNION ALL
    (
        SELECT c1, sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 2) AND (c1 <= 3) GROUP BY c1
    )
) ORDER BY c1, sum_c2 SETTINGS enable_sharding_optimize = 1, max_bytes_to_read = 1;;

SELECT /*multi_digest_e*/a.sum_c2, b.sum_c2 
FROM
    (SELECT sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 1) AND (c1 <= 2) AND (c2 > 4)) AS a
    CROSS JOIN
    (SELECT sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 2) AND (c1 <= 3) AND (c2 > 4)) AS b;

SELECT /*multi_digest_f*/a.sum_c2, b.sum_c2 
FROM
    (SELECT sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 1) AND (c1 <= 2) AND (c2 > 4)) AS a
    CROSS JOIN
    (SELECT sum(c2) AS sum_c2 FROM multi_digest_all WHERE (c1 >= 2) AND (c1 <= 3) AND (c2 > 4)) AS b;

DROP TABLE multi_digest_all;
