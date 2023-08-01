drop table IF EXISTS test_escape;

CREATE TABLE test_escape(`x` String, `y` String) ENGINE = CnchMergeTree()  PARTITION BY `x` ORDER BY `y` SETTINGS index_granularity = 8192;
INSERT INTO test_escape values ('a','_test'), ('b', '%%test'), ('c', '%test%'), ('d', '\\test\\');
SELECT x,y  from test_escape WHERE y LIKE '\_test';
SELECT x,y  from test_escape WHERE y LIKE '#_test' ESCAPE '#';
SELECT x,y  from test_escape WHERE y LIKE '@%@%test' ESCAPE '@';
SELECT x,y  from test_escape WHERE y LIKE '$%test$%' ESCAPE '$';
drop table test_escape;