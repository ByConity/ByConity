create table test_local (id UInt32, path LowCardinality(String)) engine = CnchMergeTree order by id;
WITH ((position(path, '/a') > 0) AND (NOT (position(path, 'a') > 0))) OR (path = '/b') OR (path = '/b/') as alias1 SELECT max(alias1) FROM test_local WHERE (id = 299386662);

SELECT (a = '1') OR (a = '2') OR (a like '2%') OR (a = '3') AS _1700057091683 FROM (select '1' as a);
