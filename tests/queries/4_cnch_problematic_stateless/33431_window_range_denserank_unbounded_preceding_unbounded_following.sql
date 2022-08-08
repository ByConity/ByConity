DROP DATABASE IF EXISTS test;
CREATE DATABASE test;

DROP TABLE IF EXISTS test.range_denserank_unbouned_preceding_unbounded_following;

CREATE TABLE test.range_denserank_unbouned_preceding_unbounded_following(`a` Int64, `b` Int64) 
    ENGINE = CnchMergeTree() 
    PARTITION BY `a` 
    PRIMARY KEY `a` 
    ORDER BY `a` 
    SETTINGS index_granularity = 8192;
    
INSERT INTO test.range_denserank_unbouned_preceding_unbounded_following values(0,0)(0,0)(0,1)(0,5)(0,8)(0,14)(0,14)(0,14)(0,22)(1,3)(1,3)(1,4)(1,2)(1,6)(1,7)(1,9)(1,3)(1,6);

SELECT
    DENSE_RANK() OVER (
        PARTITION BY a
        ORDER BY
            b RANGE BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
    )
FROM
    test.range_denserank_unbouned_preceding_unbounded_following
ORDER BY
    a;

DROP TABLE test.range_denserank_unbouned_preceding_unbounded_following;
