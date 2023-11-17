SET enable_optimizer = 1;
SET enable_common_expression_sharing = 1;

DROP TABLE IF EXISTS ut40066_comexp_x;

CREATE TABLE ut40066_comexp_x (a Int32, s String)
ENGINE = CnchMergeTree() order by a;

INSERT INTO ut40066_comexp_x VALUES (0, 'aabb');
INSERT INTO ut40066_comexp_x VALUES (1, 'aab ');

-- { echo }
-- test leaf segment(PREWHERE)
SET enable_common_expression_sharing_for_prewhere = 1;

EXPLAIN SELECT trim(substring(lower(s), 1, 4)) as key, count() as cnt
FROM ut40066_comexp_x
PREWHERE multiIf(length(key) = 4, 1, bar / 2 > 1, 0, 1)
WHERE ((((a + 1) * 3) % 7) as bar) = 3
GROUP BY key;

SELECT trim(substring(lower(s), 1, 4)) as key, count() as cnt
FROM ut40066_comexp_x
PREWHERE multiIf(length(key) = 4, 1, bar / 2 > 1, 0, 1)
WHERE ((((a + 1) * 3) % 7) as bar) = 3
GROUP BY key;

-- test leaf segment(non-PREWHERE)
SET enable_common_expression_sharing_for_prewhere = 0;

EXPLAIN SELECT trim(substring(lower(s), 1, 4)) as key, count() as cnt
FROM ut40066_comexp_x
PREWHERE multiIf(length(key) = 4, 1, 0)
WHERE startsWith(key, 'aa')
GROUP BY key;

SELECT trim(substring(lower(s), 1, 4)) as key, count() as cnt
FROM ut40066_comexp_x
PREWHERE multiIf(length(key) = 4, 1, 0)
WHERE startsWith(key, 'aa')
GROUP BY key;

-- test non-leaf segment
EXPLAIN SELECT trim(substring(lower(s1), 1, 4)) as key, count() as cnt
FROM
(
    SELECT max(s) AS s1 FROM ut40066_comexp_x
)
WHERE startsWith(key, 'aa')
GROUP BY key;

SELECT trim(substring(lower(s1), 1, 4)) as key, count() as cnt
FROM
(
    SELECT max(s) AS s1 FROM ut40066_comexp_x
)
WHERE startsWith(key, 'aa')
GROUP BY key;

-- test nodes with multiple chilren
SET enable_common_expression_sharing_for_prewhere = 1;

EXPLAIN SELECT trim(substring(lower(s), 1, 4)) as key, count() as cnt
FROM ut40066_comexp_x
PREWHERE multiIf(length(key) = 4, 1, bar / 2 = 1, 0, 1)
WHERE ((((a + 1) * 3) % 7) as bar) <= 3
GROUP BY key
UNION ALL
SELECT trim(substring(lower(s), 1, 4)) as key, count() as cnt
FROM ut40066_comexp_x
PREWHERE multiIf(length(key) = 4, 1, bar / 2 = 1, 0, 1)
WHERE ((((a + 1) * 3) % 7) as bar) > 3
GROUP BY key;

SELECT *
FROM (
    SELECT trim(substring(lower(s), 1, 4)) as key, count() as cnt
    FROM ut40066_comexp_x
    PREWHERE multiIf(length(key) = 4, 1, bar / 2 = 1, 0, 1)
    WHERE ((((a + 1) * 3) % 7) as bar) <= 3
    GROUP BY key
    UNION ALL
    SELECT trim(substring(lower(s), 1, 4)) as key, count() as cnt
    FROM ut40066_comexp_x
    PREWHERE multiIf(length(key) = 4, 1, bar / 2 = 1, 0, 1)
    WHERE ((((a + 1) * 3) % 7) as bar) > 3
    GROUP BY key
)
ORDER BY key;

-- test prune
SET common_expression_sharing_threshold = 10;

EXPLAIN SELECT trim(substring(lower(s), 1, 4)) as key, count() as cnt
FROM ut40066_comexp_x
PREWHERE multiIf(length(key) = 4, 1, bar / 2 > 1, 0, 1)
WHERE ((((a + 1) * 3) % 7) as bar) = 3
GROUP BY key;
