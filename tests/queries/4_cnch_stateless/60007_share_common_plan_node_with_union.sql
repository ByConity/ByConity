SET enable_optimizer = 1;
SET enable_share_common_plan_node = 1;
SET dialect_type = 'ANSI';

DROP TABLE IF EXISTS share_common_plan_node;

DROP TABLE IF EXISTS share_common_plan_node_distirubted;

CREATE TABLE share_common_plan_node (id UInt64, k2 Int64, k3 String, k4 DATE)
ENGINE = CnchMergeTree ORDER BY id;

EXPLAIN
SELECT
    NULL AS k4,
    k3,
    count(1) AS value
FROM
    share_common_plan_node AS event
WHERE
    k4 IN ('2024-04-07', '2024-04-22')
GROUP BY
    k3
UNION ALL
SELECT
    k4,
    k3,
    count(1) AS value
FROM
    share_common_plan_node AS event
WHERE
    k4 IN ('2024-04-07', '2024-04-22')
GROUP BY
    k4,
    k3;

SELECT
    NULL AS k4,
    k3,
    count(1) AS value
FROM
    share_common_plan_node AS event
WHERE
    k4 IN ('2024-04-07', '2024-04-22')
GROUP BY
    k3
UNION ALL
SELECT
    k4,
    k3,
    count(1) AS value
FROM
    share_common_plan_node AS event
WHERE
    k4 IN ('2024-04-07', '2024-04-22')
GROUP BY
    k4,
    k3;