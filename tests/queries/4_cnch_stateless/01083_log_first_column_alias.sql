DROP TABLE IF EXISTS test_alias;

CREATE TABLE test_alias (a UInt8 ALIAS b, b UInt8) ENGINE CnchMergeTree() order by tuple();

SELECT count() FROM test_alias;

DROP TABLE test_alias;
