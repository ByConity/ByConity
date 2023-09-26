DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt8) ENGINE = CnchMergeTree() ORDER BY x;

INSERT INTO test SELECT 1 AS x;
INSERT INTO test SELECT 1 AS x SETTINGS extremes = 1;
INSERT INTO test SELECT 1 AS x GROUP BY 1 WITH TOTALS SETTINGS enable_optimizer = 0;  -- TODO: wangtao
INSERT INTO test SELECT 1 AS x GROUP BY 1 WITH TOTALS SETTINGS extremes = 1, enable_optimizer = 0; -- TODO: wangtao

SELECT count(), min(x), max(x) FROM test;

DROP TABLE test;
