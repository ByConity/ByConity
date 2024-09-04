

DROP TABLE IF EXISTS test_map_implicit_column;
DROP TABLE IF EXISTS test_map_implicit_column2;

CREATE TABLE test_map_implicit_column(`id` Int32, `m` Map(String, Int32))
    ENGINE = CnchMergeTree()
    PARTITION BY `id`
    PRIMARY KEY `id`
    ORDER BY `id`
    SETTINGS index_granularity = 8192;

CREATE TABLE test_map_implicit_column2(`id` Int32, `m2` Map(String, Int32))
    ENGINE = CnchMergeTree()
    PARTITION BY `id`
    PRIMARY KEY `id`
    ORDER BY `id`
    SETTINGS index_granularity = 8192;

insert into test_map_implicit_column values (1, {'foo':1, 'bar':10}) (2, {'foo':10, 'bar':1});
insert into test_map_implicit_column2 values (1, {'foo':2, 'bar':11}) (2, {'foo':11, 'bar':2});

SELECT 'case 1';
SELECT id, m{'foo'} FROM test_map_implicit_column WHERE m{'bar'} + 1 > 10 ORDER BY id;

SELECT 'case 2';
SELECT id, x{'bar'}, y
FROM (
    SELECT id, m AS x, m{'foo'} AS y
    FROM test_map_implicit_column
    )
ORDER BY id;

SELECT 'case 5';
SELECT
    id,
    m3 AS m,
    m4 AS m2,
    a,
    b
FROM (
         SELECT t1.id AS id, m{'foo'} AS a, m2{'bar'} AS b, m AS m3, m2 AS m4
         FROM test_map_implicit_column t1 JOIN test_map_implicit_column2 t2 ON t1.id = t2.id
         ORDER BY m{'foo'}, t1.m{'foo'}
     )
ORDER BY m{'foo'};

-- TODO: Unexpected type Map(String, Int32) of argument of function exchangeHash
-- SELECT 'case 6';
-- SELECT id, m{'bar'}
-- FROM test_map_implicit_column
-- WHERE m{'foo'} > 1
-- GROUP BY id, m
-- ORDER BY id;

SELECT 'case 7';
SELECT id, m{'bar'}
FROM test_map_implicit_column
WHERE m{'foo'} > 1
GROUP BY id, m{'bar'}
ORDER BY id;

-- TODO: Map doesn't support updateHashWithValue.
-- SELECT 'case 8';
-- SELECT t1.id, m{'foo'}
-- FROM test_map_implicit_column AS t1 JOIN test_map_implicit_column AS t2
-- USING (m)
-- ORDER BY t1.id;

SELECT 'case 9';
SELECT id, m{'foo'}
FROM (
    SELECT * FROM test_map_implicit_column
    UNION ALL
    SELECT * FROM test_map_implicit_column
    )
ORDER BY id;

DROP TABLE IF EXISTS test_map_implicit_column;
DROP TABLE IF EXISTS test_map_implicit_column2;
