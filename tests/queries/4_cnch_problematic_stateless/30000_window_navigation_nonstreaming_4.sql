DROP DATABASE IF EXISTS test;
CREATE DATABASE test;

DROP TABLE IF EXISTS test.window_navigation;

CREATE TABLE test.window_navigation(`x` UInt64, `y` UInt64, `arr` Array(Float64),`fl` Float64, `dt` DateTime('UTC'), `cat` String)
    ENGINE = CnchMergeTree() 
    ORDER BY `x`
    SETTINGS index_granularity = 8192;

INSERT INTO test.window_navigation values (1, 3, [3,4], 0.65, '2019-01-02 00:00:00', 'aa'), (1, 4, [4,4], 0.66, '2019-01-03 00:00:00', 'bb'), (2, 3, [1,4,5], 0.69, '2019-01-12 00:00:00', 'cc'), (2, 5, [3,4], 0.611, '2019-11-02 00:00:00', 'aa'), (1, 2, [1,2], 0.64, '2019-01-01 00:00:00', 'aaaaaa');

SELECT
  x,
  y,
  fl,
  dt,
  arr,
  LEAD(y) OVER (PARTITION BY x ORDER BY y) as a,
  LEAD(y, 2) OVER (PARTITION BY x ORDER BY y) as b,
  LEAD(y, 2, 3) OVER (PARTITION BY x ORDER BY y) as c,
  LEAD(y*x+1, 1, 99) OVER (PARTITION BY x ORDER BY y) as d,
  LEAD(arr, 1, [1,2,3]) OVER (PARTITION BY x ORDER BY y) as e,
  LEAD(dt, 1, 12345678) OVER (PARTITION BY x ORDER BY y) as f,
  LEAD(fl, 1, 0.99) OVER (PARTITION BY x ORDER BY y) as g,
  LEAD(cat, 1, 'lead_function_default_value_test') OVER (PARTITION BY x ORDER BY y) as h 
FROM test.window_navigation
ORDER BY x, y, fl, dt, arr
FORMAT TabSeparated;

DROP TABLE test.window_navigation;