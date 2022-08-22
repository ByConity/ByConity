DROP DATABASE IF EXISTS test;


DROP TABLE IF EXISTS window_navigation;

CREATE TABLE window_navigation(`a` Int64, `b` Int64) 
    ENGINE = CnchMergeTree() 
    PARTITION BY `a` 
    PRIMARY KEY `a` 
    ORDER BY `a` 
    SETTINGS index_granularity = 8192;
    
INSERT INTO window_navigation values(1,1)(1,2)(1,4)(2,4)(2,4)(2,4)(2,4)(2,4)(2,4)(2,4)(2,4)(2,5)(3,3)(4,4)(4,8);

set max_threads=1;
SELECT
  a,
  b,
  PERCENT_RANK() OVER (PARTITION BY a ORDER BY a),
  CUME_DIST() OVER (PARTITION by b ORDER BY b),
  NTILE(2 * 1 + 1) OVER (PARTITION BY a ORDER BY b),
  NTILE(4) OVER (PARTITION BY a ORDER BY b)
FROM window_navigation
ORDER BY a, b, 5, 6;

DROP TABLE window_navigation;