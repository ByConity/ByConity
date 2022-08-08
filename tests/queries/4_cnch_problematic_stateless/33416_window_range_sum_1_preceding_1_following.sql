DROP DATABASE IF EXISTS test;
CREATE DATABASE test;

DROP TABLE IF EXISTS test.range_sum_1_preceding_1_following;

CREATE TABLE test.range_sum_1_preceding_1_following(`a` Int64, `b` Int64) 
    ENGINE = CnchMergeTree() 
    PARTITION BY `a` 
    PRIMARY KEY `a` 
    ORDER BY `a` 
    SETTINGS index_granularity = 8192;
    
INSERT INTO test.range_sum_1_preceding_1_following values(0,0)(0,0)(0,1)(0,5)(0,8)(0,14)(0,14)(0,14)(0,22)(1,3)(1,3)(1,4)(1,2)(1,6)(1,7)(1,9)(1,3)(1,6);

select a, sum(b) over (partition by a order by b RANGE BETWEEN 1 PRECEDING AND 1 FOLLOwING) as res
FROM test.range_sum_1_preceding_1_following
order by a, res;

DROP TABLE test.range_sum_1_preceding_1_following;
