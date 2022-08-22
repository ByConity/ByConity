DROP DATABASE IF EXISTS test;


DROP TABLE IF EXISTS rows_denserank_unbounded_preceding_current_row;

CREATE TABLE rows_denserank_unbounded_preceding_current_row(`a` Int64, `b` Int64) 
    ENGINE = CnchMergeTree() 
    PARTITION BY `a` 
    PRIMARY KEY `a` 
    ORDER BY `a` 
    SETTINGS index_granularity = 8192;
    
INSERT INTO rows_denserank_unbounded_preceding_current_row values(0,0)(0,0)(0,1)(0,5)(0,8)(0,14)(0,14)(0,14)(0,22)(1,3)(1,3)(1,4)(1,2)(1,6)(1,7)(1,9)(1,3)(1,6);

select DENSE_RANK() over (partition by a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM  rows_denserank_unbounded_preceding_current_row;

DROP TABLE rows_denserank_unbounded_preceding_current_row;
