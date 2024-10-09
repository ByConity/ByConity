DROP TABLE IF EXISTS t40075_subcol_cte;

CREATE TABLE t40075_subcol_cte
(
    a Int32,
    m Map(String, String)
) ENGINE = CnchMergeTree() ORDER by a;


EXPLAIN
WITH cte AS
(
    SELECT
       m AS x,
       m{'foo'} as y
    FROM t40075_subcol_cte
)
SELECT
    count()
FROM
(
    SELECT
        t3.*,
        t4.x AS t4x
    FROM
    (
        SELECT
            t1.x AS t1x,
            t2.x AS t2x,
            t1.y AS t1y,
            t2.y AS t2y
        FROM cte t1, cte t2
        WHERE t1.x{'aa'} =  t2.x{'bb'}
    ) t3,
    (
        SELECT x
        FROM cte tt1
        UNION ALL
        SELECT x
        FROM cte tt2
    ) t4
    WHERE t1y = t4.x{'cc'}
) t5,
(
    SELECT
        x,
        x{'cc'} AS y
    FROM cte
    UNION ALL
    SELECT
        map('1', 'a', '2', 'b') AS x,
        'zz' AS y
) t6
WHERE t2y = t6.x{'dd'} AND t6.y != ''
SETTINGS enable_optimizer=1, cte_mode='SHARED', max_buffer_size_for_deadlock_cte=-1;

SELECT '';

EXPLAIN
WITH cte AS
(
    SELECT
       m AS x,
       m{'foo'} as y
    FROM t40075_subcol_cte
)
SELECT
    count()
FROM
(
    SELECT
        t3.*,
        t4.x AS t4x
    FROM
    (
        SELECT
            t1.x AS t1x,
            t2.x AS t2x,
            t1.y AS t1y,
            t2.y AS t2y
        FROM cte t1, cte t2
        WHERE t1.x{'aa'} =  t2.x{'bb'}
    ) t3,
    (
        SELECT x
        FROM cte tt1
        UNION ALL
        SELECT x
        FROM cte tt2
    ) t4
    WHERE t1y = t4.x{'cc'}
) t5,
(
    SELECT
        x,
        x{'cc'} AS y
    FROM cte
    UNION ALL
    SELECT
        map('1', 'a', '2', 'b') AS x,
        'zz' AS y
) t6
WHERE t2y = t6.x{'dd'} AND t6.y != ''
SETTINGS enable_optimizer=1, cte_mode='INLINED';
