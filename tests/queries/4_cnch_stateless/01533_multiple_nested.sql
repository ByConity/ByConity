DROP TABLE IF EXISTS nested;

SET flatten_nested = 0;

CREATE TABLE nested
(
    id Int64,
    col1 Nested(a UInt32, s String),
    col2 Nested(a UInt32, n Nested(s String, b UInt32)),
    col3 Nested(n1 Nested(a UInt32, b UInt32), n2 Nested(s String, t String))
)
ENGINE = CnchMergeTree 
ORDER BY id;

INSERT INTO nested VALUES (1, [(1, 'q'), (2, 'w'), (3, 'e')], [(4, [('a', 5), ('s', 6), ('d', 7)])], [([(8, 9), (10, 11)], [('z', 'x'), ('c', 'v')])]);
INSERT INTO nested VALUES (2, [(12, 'qq')], [(4, []), (5, [('b', 6), ('n', 7)])], [([], []), ([(44, 55), (66, 77)], [])]);

-- OPTIMIZE TABLE nested FINAL;

SELECT 'all';
SELECT * FROM nested ORDER BY id;
SELECT 'col1';
SELECT col1.a, col1.s FROM nested ORDER BY id;
SELECT 'col2';
SELECT col2.a, col2.n, col2.n.s, col2.n.b FROM nested ORDER BY id;
SELECT 'col3';
SELECT col3.n1, col3.n2, col3.n1.a, col3.n1.b, col3.n2.s, col3.n2.t FROM nested ORDER BY id;

SELECT 'read files';

SYSTEM DROP MARK CACHE;
SELECT col1.a FROM nested FORMAT Null;

SYSTEM DROP MARK CACHE;
SELECT col3.n2.s FROM nested FORMAT Null;

DROP TABLE nested;

CREATE TABLE nested
(
    id UInt32,
    col1 Nested(a UInt32, n Nested(s String, b UInt32))
)
ENGINE = CnchMergeTree 
ORDER BY id;

INSERT INTO nested SELECT number, arrayMap(x -> (x, arrayMap(y -> (toString(y * x), y + x), range(number % 17))), range(number % 19)) FROM numbers(1000000);
SELECT id % 10, sum(length(col1)), sumArray(arrayMap(x -> length(x), col1.n.b)) FROM nested GROUP BY id % 10 ORDER BY id % 10;

SELECT arraySum(col1.a), arrayMap(x -> x * x * 2, col1.a) FROM nested ORDER BY id LIMIT 5;
SELECT untuple(arrayJoin(arrayJoin(col1.n))) FROM nested ORDER BY id LIMIT 10 OFFSET 10;

DROP TABLE IF EXISTS nested;