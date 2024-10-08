DROP TABLE IF EXISTS t40034_t1;
DROP TABLE IF EXISTS t40034_t2;
DROP TABLE IF EXISTS t40034_t3;

CREATE TABLE t40034_t1(int1 Int32, str1 String, `map1` Map(String, Int64), `map2` Map(String, Int64)) ENGINE = CnchMergeTree() ORDER BY tuple();
CREATE TABLE t40034_t2(int2 Int32, str2 String, `map3` Map(String, Int64), `map4` Map(String, Int64)) ENGINE = CnchMergeTree() ORDER BY tuple();
CREATE TABLE t40034_t3(int3 Int32, str3 String, `map5` Map(String, Int64), `map6` Map(String, Int64)) ENGINE = CnchMergeTree() ORDER BY tuple();

INSERT INTO t40034_t1 VALUES (100, 'str1', {'a': 1, 'b': 2, 'c': 5, 'd': 6, 'e': 7, 'f': 8}, {'x': 3, 'y': 4, 'z': 9, 'p': 10, 'q': 11});
INSERT INTO t40034_t2 VALUES (200, 'str2', {'x': 30, 'y': 40, 'z': 90, 'p': 100, 'q': 110}, {'a': 10, 'b': 20, 'c': 50, 'd': 60, 'e': 70, 'f': 80});
INSERT INTO t40034_t3 VALUES (300, 'str3', {'a': 100, 'b': 200, 'c': 500, 'd': 600, 'e': 700, 'f': 800}, {'x': 300, 'y': 400, 'z': 900, 'p': 1000, 'q': 1100});

SET enable_optimizer=1;

EXPLAIN SELECT mapElement(map1, 'a') FROM t40034_t1;
SELECT mapElement(map1, 'a') FROM t40034_t1;

EXPLAIN SELECT mapElement(m, 'a') FROM (SELECT map1 as m FROM t40034_t1);
SELECT mapElement(m, 'a') FROM (SELECT map1 as m FROM t40034_t1);

EXPLAIN SELECT str1, int1, mapElement(map1, 'a') FROM (SELECT int1, str1, map1 FROM t40034_t1);
SELECT str1, int1, mapElement(map1, 'a') FROM (SELECT int1, str1, map1 FROM t40034_t1);

SET enable_subcolumn_optimization_through_union=1;

EXPLAIN SELECT
    c1, c2, mapElement(c4, 'y')
FROM
    (
        (SELECT int1 as c1, str1 as c2, map1 as c3, map2 as c4 FROM t40034_t1)
        UNION ALL
        (SELECT int2, str2, map4, map3 FROM t40034_t2)
    )
WHERE mapElement(c3, 'a') > 1;

SELECT
    c1, c2, mapElement(c4, 'y')
FROM
(
    (SELECT int1 as c1, str1 as c2, map1 as c3, map2 as c4 FROM t40034_t1)
    UNION ALL
    (SELECT int2, str2, map4, map3 FROM t40034_t2)
)
WHERE mapElement(c3, 'a') > 1;

EXPLAIN SELECT
    c1, c2, mapElement(c6, 'a')
FROM
    (
        SELECT
            c1, c2, mapElement(c4, 'x') as c5, c3 as c6
        FROM
            (
                (SELECT int1 as c1, str1 as c2, map1 as c3, map2 as c4 FROM t40034_t1)
                UNION ALL
                (SELECT int2, str2, map4, map3 FROM t40034_t2)
            )
        UNION ALL
        SELECT
            c1, c2, mapElement(c3, 'a') as c5, c4 as c6
        FROM
            (
                (SELECT int1 as c1, str1 as c2, map1 as c3, map2 as c4 FROM t40034_t1)
                UNION ALL
                (SELECT int2, str2, map4, map3 FROM t40034_t2)
            )
    )
WHERE c5 > 1;

SELECT
    c1, c2, mapElement(c6, 'a')
FROM
(
    SELECT
    c1, c2, mapElement(c4, 'x') as c5, c3 as c6
    FROM
    (
        (SELECT int1 as c1, str1 as c2, map1 as c3, map2 as c4 FROM t40034_t1)
        UNION ALL
        (SELECT int2, str2, map4, map3 FROM t40034_t2)
    )
    UNION ALL
    SELECT
    c1, c2, mapElement(c3, 'a') as c5, c4 as c6
    FROM
    (
        (SELECT int1 as c1, str1 as c2, map1 as c3, map2 as c4 FROM t40034_t1)
        UNION ALL
        (SELECT int2, str2, map4, map3 FROM t40034_t2)
    )
)
WHERE c5 > 1
ORDER BY c1, c2, mapElement(c6, 'a');

SELECT
    mapElement(m1, 'a'),
    mapElement(m2, 'y'),
    mapElement(m2, 'z'),
    mapElement(m1, 'c'),
    mapElement(m1, 'b'),
    mapElement(m2, 'x'),
    mapElement(m1, 'e'),
    mapElement(m1, 'f'),
    mapElement(m2, 'q')
FROM
(
    SELECT 0, mapElement(map1, 'c'), map1 as m1, mapElement(map2, 'u3'), map2 as m2
    FROM t40034_t1
    WHERE int1 > 0 OR (mapElement(map1, 'u4') > 0 AND mapElement(map1, 'u5') + mapElement(map2, 'd') < 1)
    UNION ALL
    SELECT mapElement(map3, 'u1'), mapElement(map3, 'u2'), map4, mapElement(map3, 'x'), map3
    FROM t40034_t2
    WHERE int2 > 0 OR (mapElement(map3, 'u7') < mapElement(map4, 'u8'))
    UNION ALL
    SELECT mapElement(map6, 'u1'), mapElement(map5, 'u2'), map5, mapElement(map5, 'x'), map6
    FROM t40034_t3
    WHERE int3 > 0 OR (mapElement(map6, 'u3') > 0 AND mapElement(map5, 'u7') < mapElement(map6, 'u8'))
)
ORDER BY mapElement(m1, 'a');
