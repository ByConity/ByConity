USE test;
DROP TABLE IF EXISTS test.data;
CREATE TABLE test.data (s String, n UInt32) ENGINE = CnchMergeTree() PARTITION BY n ORDER BY n;

SET free_resource_early_in_write = 1;
INSERT INTO test.data VALUES ('hello', 1);
select * FROM test.data;

DROP TABLE test.data;