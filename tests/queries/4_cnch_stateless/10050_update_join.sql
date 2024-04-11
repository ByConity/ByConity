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
