drop table if exists update_join_test;

CREATE table update_join_test(
    id Int32, 
    name String, 
    age Int32, 
    addr String
) ENGINE = CnchMergeTree()
PARTITION BY xxHash64(name) % 10
ORDER BY id
UNIQUE KEY id;

CREATE table update_join_test_new(
    id Int32, 
    name String, 
    age Int32, 
    addr String
) ENGINE = CnchMergeTree()
PARTITION BY xxHash64(name) % 10
ORDER BY id
UNIQUE KEY id;

INSERT INTO update_join_test VALUES (1, 'zhao', 1, 'cn'), (2, 'qian', 2, 'cn'), (3, 'sun', 3, 'cn'), (4, 'li', 4, 'cn');
INSERT INTO update_join_test_new VALUES (1, 'ZHAO', 10, 'bj'), (2, 'qian', 20, 'bj'), (4, 'LI', 40, 'bj'), (5, 'ZHOU', 5, 'bj');

UPDATE update_join_test INNER JOIN update_join_test_new ON update_join_test.name = update_join_test_new.name SET addr = 'bj';

-- res: cn, bj, cn, cn
SELECT addr FROM update_join_test ORDER BY id;

UPDATE update_join_test INNER JOIN update_join_test_new ON update_join_test.id = update_join_test_new.id SET age = update_join_test_new.age;

-- res: 10, 20, 3, 40
SELECT age FROM update_join_test ORDER BY id;

DROP TABLE update_join_test;
DROP TABLE update_join_test_new;

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.update_join_tuj;
DROP TABLE IF EXISTS test.update_join_tt;

CREATE TABLE test.update_join_tuj(k Int32, m Int32, m2 Int32) ENGINE = CnchMergeTree ORDER BY k UNIQUE KEY k;
CREATE TABLE test.update_join_tt(k Int32, m Int32) ENGINE = CnchMergeTree ORDER BY k;

INSERT INTO test.update_join_tuj SELECT number, number, number FROM numbers(5);
INSERT INTO test.update_join_tt VALUES (1, 10), (2, 20), (4, 40), (6, 60);

SELECT 'Try all combinations';

UPDATE test.update_join_tuj SET m = m * 2;
UPDATE test.update_join_tuj SET m = 2;
UPDATE test.update_join_tuj SET m = 2 WHERE k >= 3;
UPDATE test.update_join_tuj AS a SET m = 3;
UPDATE test.update_join_tuj AS a SET a.m = 3;
UPDATE test.update_join_tuj AS a LEFT JOIN test.update_join_tt AS b ON a.k = b.k SET m = b.m WHERE k >= 1;
UPDATE test.update_join_tuj AS a LEFT JOIN test.update_join_tt AS b ON a.k = b.k SET a.m = b.m, a.m2 = b.m * 10 WHERE k >= 1;

-- No table alias found: x
UPDATE test.update_join_tuj SET x.m = 2; -- { serverError 36 }
-- only allowed to update the first table
UPDATE test.update_join_tuj AS a SET b.m = 3; -- { serverError 36 }
-- UPDATE multi tables is not supported.
UPDATE test.update_join_tuj AS a LEFT JOIN test.update_join_tt AS b ON a.k = b.k SET a.m = b.m, b.m = b.m * 10 WHERE k >= 1; -- { serverError 36 }
-- only allowed to update the first table
UPDATE test.update_join_tuj AS a LEFT JOIN test.update_join_tt AS b ON a.k = b.k SET b.m = a.m WHERE k >= 1; -- { serverError 36 }

SELECT m, m2 FROM test.update_join_tuj ORDER BY k;

DROP TABLE test.update_join_tuj;
DROP TABLE test.update_join_tt;
