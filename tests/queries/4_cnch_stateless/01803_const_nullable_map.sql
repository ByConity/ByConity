DROP TABLE IF EXISTS t_map_null;

SET allow_experimental_map_type = 1;

CREATE TABLE t_map_null (a Map(String, String) KV, b String) engine = CnchMergeTree ORDER BY tuple();
INSERT INTO t_map_null VALUES (map('a', 'b', 'c', 'd'), 'foo');
SELECT count() FROM t_map_null WHERE a = map('name', NULL, '', NULL);

DROP TABLE t_map_null;
CREATE TABLE t_map_null (a Map(String, String) BYTE, b String) engine = CnchMergeTree ORDER BY tuple();
INSERT INTO t_map_null VALUES (map('a', 'b', 'c', 'd'), 'foo');
SELECT count() FROM t_map_null WHERE a = map('name', NULL, '', NULL);

DROP TABLE t_map_null;
