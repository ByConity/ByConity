CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.gc;

CREATE TABLE test.gc
(
    id Int32,
    name String,
    birthday Nullable(Date),
    gid UInt32
)
ENGINE = CnchMergeTree()
PARTITION BY id
ORDER BY id;

INSERT INTO test.gc VALUES (1, 'Anna', '2001-01-01', 1), (2, 'Bob', '2002-02-02', 1), (3, 'Charlie', '2003-03-03', 1), (4, 'Dan', NULL, 2), (5, 'Eve', '2004-04-04', 2), (6, 'Frans', NULL, 3), (7, 'Anna', NULL, 4);

SELECT groupConcat(name) FROM (SELECT * FROM test.gc ORDER BY id);
SELECT groupConcat(name) FROM (SELECT * FROM test.gc ORDER BY id) GROUP BY gid ORDER BY gid;
SELECT groupConcat('#')(name) FROM (SELECT * FROM test.gc ORDER BY id);
SELECT groupConcat('+')(DISTINCT name) FROM (SELECT * FROM test.gc ORDER BY id);
SELECT groupConcat('#') FROM (SELECT * FROM test.gc ORDER BY id);
SELECT groupConcat(DISTINCT '#') FROM test.gc;

SELECT GROUP_CONCAT(DISTINCT name SEPARATOR '#') FROM (SELECT * FROM test.gc ORDER BY id);

SELECT GROUP_CONCAT(DISTINCT name ORDER BY name DESC SEPARATOR '#') FROM test.gc;
SELECT GROUP_CONCAT(DISTINCT name ORDER BY name DESC, id ASC SEPARATOR '#') FROM test.gc;
SELECT GROUP_CONCAT(DISTINCT name ORDER BY name DESC, id DESC SEPARATOR '#') FROM test.gc;
SELECT GROUP_CONCAT(DISTINCT name ORDER BY name DESC, id ASC SEPARATOR '#'), GROUP_CONCAT(DISTINCT name ORDER BY name DESC, id DESC SEPARATOR '#') FROM test.gc;

-- Mysql compatibility
SELECT GROUP_CONCAT(DISTINCT name ORDER BY BINARY name DESC SEPARATOR '#') FROM test.gc;
