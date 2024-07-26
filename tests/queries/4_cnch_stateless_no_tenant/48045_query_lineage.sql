use test;
DROP TABLE IF EXISTS lineage_test;
DROP TABLE IF EXISTS lineage_test2;
CREATE TABLE lineage_test (a UInt32, b UInt32) ENGINE = CnchMergeTree() partition by a order by a;
CREATE TABLE lineage_test2 (a UInt32, b UInt32, c Nullable(UInt64)) ENGINE = CnchMergeTree() partition by a order by a;

set cte_mode='SHARED';

explain metadata format_json=1, lineage=1
WITH t3 AS
    (
        SELECT
            count() AS a,
            sum(b) AS b
        FROM lineage_test2
        GROUP BY c
    )
SELECT
    a,
    b,
    2
FROM
(
    (
        SELECT
            t3.b AS a,
            t2.b AS b
        FROM lineage_test AS t2
        INNER JOIN t3 ON t2.a = t3.a
    )
    UNION ALL
    (
        SELECT
            *
        FROM t3
    )
) AS t1
WHERE t1.a = 1
FORMAT TabSeparatedRaw;

explain metadata format_json=1, lineage=1
select
    distinct t1.a, t2.b
from lineage_test t1 join lineage_test2 t2 on t1.a=t2.a
where t1.a < 10
FORMAT TabSeparatedRaw;

explain metadata format_json=1, lineage_use_optimizer=1
select
    distinct t1.a, t2.b
from lineage_test t1 join lineage_test2 t2 on t1.a=t2.a
where t1.a < 10
FORMAT TabSeparatedRaw;

explain metadata format_json=1, lineage=1
select
    distinct t1.a, t2.b
from cnch(server, test, lineage_test) t1 join lineage_test2 t2 on t1.a=t2.a
where t1.a < 10
FORMAT TabSeparatedRaw;

explain metadata format_json=1, lineage=1
insert into lineage_test(a,b)
select
    c,
    a
from lineage_test2
where c is not null
group by a,c;

DROP TABLE IF EXISTS lineage_test;
DROP TABLE IF EXISTS lineage_test2;
