


DROP TABLE IF EXISTS empty_test;

CREATE TABLE empty_test (d UInt8 Default 1, mapping Map(String, UInt64)) engine=CnchMergeTree Order by d;
INSERT INTO empty_test (mapping) VALUES ({'c1':1001}), ({'c2':1002}), ({});
SELECT empty(mapping) from empty_test;

DROP TABLE empty_test;

