drop table if exists t40037;

create table t40037
(
    i1 Int32,
    i2 Int32,
    int_array Array(Int32)
) ENGINE = CnchMergeTree() ORDER BY tuple();

INSERT INTO t40037 VALUES (1, 2, [1, 2]);

set enable_optimizer = 1;

-- use prewhere to force MergeTreeRangeReader reorder columns
SELECT
    i1, count()
FROM t40037
PREWHERE i1 = 1
WHERE arraySetCheck(int_array, 1)
GROUP BY i1;
