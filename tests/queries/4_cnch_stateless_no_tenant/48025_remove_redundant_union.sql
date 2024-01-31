drop database if exists zx_test;
create database if not exists zx_test;
use zx_test;
drop table if exists table1;
drop table if exists table2;
drop table if exists table3;
drop table if exists table4;
drop table if exists table5;
drop table if exists table6;
drop table if exists table1_local;
drop table if exists table2_local;
drop table if exists table3_local;
drop table if exists table4_local;
drop table if exists table5_local;
drop table if exists table6_local;


CREATE TABLE zx_test.table1 (
    recvtime DateTime,
    i_7id Nullable(String),
    uykyub Map(String, String)
) ENGINE = CnchMergeTree()
order by
    recvtime;

CREATE TABLE zx_test.table2 (
    my_date Date,
    k1k1k1 String,
    k2k2k2 Nullable(String),
    k4k4k4 String
) ENGINE = CnchMergeTree()
order by
    k1k1k1;

CREATE TABLE zx_test.table3 (
    my_date Date,
    v2v2v2 Int64
) ENGINE = CnchMergeTree()
order by
    v2v2v2;

CREATE TABLE zx_test.table4 (
    i_7id Nullable(String),
    ppp Nullable(String),
    my_date Date,
    mmm Nullable(String)
) ENGINE = CnchMergeTree()
order by
    my_date;

CREATE TABLE zx_test.table5 (
    y_time DateTime,
    pii Nullable(String),
    hvbqqa Nullable(String),
    recvtime DateTime,
    hhhhasdfasffsad Nullable(String),
    hhhh Nullable(String),
    oooo Nullable(String),
    os Nullable(String),
    i_7id Nullable(String),
    ggh4 Map(String, String),
    dsafghh Map(String, String),
    uykyub Map(String, String)
) ENGINE = CnchMergeTree()
order by
    y_time;

CREATE TABLE zx_test.table6 (
    status Nullable(String),
    v2v2v2 Nullable(Int64),
    vvv1 String
) ENGINE = CnchMergeTree()
order by
    vvv1;

drop view if exists zx_test.oneview;

CREATE VIEW zx_test.oneview (
    ppp Nullable(String),
    my_date Date,
    mmm Nullable(String)
) AS
SELECT
    t1.ppp AS ppp,
    t1.my_date AS my_date,
    t1.mmm AS mmm
FROM
    (
        SELECT
            'PPPAAA' AS ppp,
            a.my_date AS my_date,
            a.mmm AS mmm,
            a.i_7id as i_7id
        FROM
            (
                SELECT
                    123 AS v2v2v2,
                    i_7id,
                    toDate(recvtime) AS my_date,
                    uykyub {'mmm'} AS mmm
                FROM
                    zx_test.table1
                WHERE
                    toDate(recvtime) = (toDate('2018-08-08') + toIntervalDay(0))
            ) AS a
            LEFT JOIN (
                SELECT
                    v2v2v2
                FROM
                    zx_test.table3
                WHERE
                    (
                        my_date = (
                            SELECT
                                max(my_date)
                            FROM
                                zx_test.table3
                        )
                    )
                    AND (v2v2v2 > 0)
                GROUP BY
                    v2v2v2
            ) AS d ON a.v2v2v2 = d.v2v2v2
            LEFT JOIN (
                SELECT
                    v2v2v2
                FROM
                    zx_test.table6
                WHERE
                    (
                        vvv1 = (
                            SELECT
                                max(vvv1)
                            FROM
                                zx_test.table6
                        )
                    )
                    AND (status = '1')
                    AND isNotNull(v2v2v2)
                GROUP BY
                    v2v2v2
            ) AS f ON a.v2v2v2 = f.v2v2v2
        WHERE
            isNull(f.v2v2v2)
        UNION
        ALL
        SELECT
            a.ppp AS ppp,
            a.my_date AS my_date,
            a.mmm AS mmm,
            a.i_7id as i_7id
        FROM
            (
                SELECT
                    888 AS v2v2v2,
                    c.i_7id AS i_7id,
                    multiIf(
                        c.pii = '132 06',
                        'WXVAT',
                        (c.pii = '631')
                        AND (c.hvbqqa = 'fas'),
                        'PDIN',
                        (c.pii = '63')
                        AND (c.hvbqqa = 'fdsaf'),
                        'PCDT',
                        (c.pii = '75')
                        OR(c.hvbqqa = 'zvc'),
                        'XXX',
                        NULL
                    ) AS ppp,
                    toDate(c.recvtime) AS my_date,
                    c.oooo AS mmm
                FROM
                    zx_test.table5 AS c
                WHERE
                    (
                        toDate(c.recvtime) = (toDate('2018-08-08') + toIntervalDay(0))
                    )
            ) AS a
            LEFT JOIN (
                SELECT
                    v2v2v2
                FROM
                    zx_test.table6
                WHERE
                    (
                        vvv1 = (
                            SELECT
                                max(vvv1)
                            FROM
                                zx_test.table6
                        )
                    )
                    AND (status = '1')
                    AND isNotNull(v2v2v2)
                GROUP BY
                    v2v2v2
            ) AS f ON a.v2v2v2 = f.v2v2v2
        WHERE
            isNull(f.v2v2v2)
        UNION
        ALL
        SELECT
            b.ppp,
            b.my_date,
            b.mmm,
            b.i_7id
        FROM
            zx_test.table4 AS b
        WHERE
            b.my_date <= (toDate('2018-08-09') + toIntervalDay(-1))
    ) As t1
    LEFT JOIN (
        SELECT
            k1k1k1,
            k2k2k2,
            k4k4k4
        FROM
            zx_test.table2
        WHERE
            (
                my_date IN (
                    SELECT
                        max(my_date)
                    FROM
                        zx_test.table2
                )
            )
        GROUP BY
            k1k1k1,
            k2k2k2,
            k4k4k4
    ) AS t3 ON (t1.i_7id = t3.k1k1k1)
    AND (t1.ppp = t3.k4k4k4);

set enable_optimizer=1;
explain SELECT
    DISTINCT mmm
FROM
    oneview
WHERE
    (my_date >= '2021-01-22')
    AND (my_date <= '2021-01-22')
    AND (ppp = 'XXX');