DROP TABLE IF EXISTS 51005_share_common_plan_node_join;

CREATE TABLE 51005_share_common_plan_node_join (`id` UInt32, `k1` UInt32, `k2` String) ENGINE = CnchMergeTree
ORDER BY id;

-- not hint
SELECT
    subquery1.id,
    subquery1.t1k1,
    subquery2.t1k1,
    subquery2.t2k1
FROM
    (
        SELECT
            t1.id,
            t1.k1 t1k1
        FROM
            51005_share_common_plan_node_join t1
            LEFT JOIN 51005_share_common_plan_node_join t2 ON t1.id = t2.id
            AND t1.k1 = t2.k1
    ) subquery1
    LEFT JOIN (
        SELECT
            t1.id,
            t1.k1 t1k1,
            t2.k1 t2k1
        FROM
            51005_share_common_plan_node_join t1
            LEFT JOIN 51005_share_common_plan_node_join t2 ON t1.id = t2.id
            AND t1.k1 = t2.k1
    ) subquery2 ON subquery1.id = subquery2.id settings enable_share_common_plan_node = 1,
    max_buffer_size_for_deadlock_cte = -1,
    cte_mode = 'SHARED';

-- hint share common plan node
SELECT
    subquery1.id,
    subquery1.t1k1,
    subquery2.t1k1
FROM
    (
        SELECT
            t1.id,
            t1.k1 t1k1
        FROM
            51005_share_common_plan_node_join t1
            LEFT JOIN 51005_share_common_plan_node_join t2 ON t1.id = t2.id
            AND t1.k1 = t2.k1
    ) subquery1
    LEFT JOIN (
        SELECT
            t1.id,
            t1.k1 t1k1
        FROM
            51005_share_common_plan_node_join t1
            LEFT JOIN 51005_share_common_plan_node_join t2 ON t1.id = t2.id
            AND t1.k1 = t2.k1
    ) subquery2 ON subquery1.id = subquery2.id settings enable_share_common_plan_node = 1,
    max_buffer_size_for_deadlock_cte = -1,
    cte_mode = 'SHARED';
