DROP TABLE IF EXISTS test.00745_merge_tree_map1;
DROP TABLE IF EXISTS test.00745_merge_tree_map2;

SET mutations_sync = 1;

SELECT 'test byte map type and kv map type';

CREATE TABLE test.00745_merge_tree_map1 (n UInt8, m1 Map(String, String), m2 Map(LowCardinality(String), LowCardinality(Nullable(String)))) Engine=CnchMergeTree ORDER BY n;
CREATE TABLE test.00745_merge_tree_map2 (n UInt8, m1 Map(String, String) KV, m2 Map(LowCardinality(String), LowCardinality(Nullable(String))) KV) Engine=CnchMergeTree ORDER BY n;

SYSTEM START MERGES test.00745_merge_tree_map1;
SYSTEM START MERGES test.00745_merge_tree_map2;

-- write parts to test.00745_merge_tree_map1
INSERT INTO test.00745_merge_tree_map1 VALUES (1, {'k1': 'v1'}, {'k1': 'v1'});
SELECT n, m1{'k1'}, m2{'k1'} FROM test.00745_merge_tree_map1;

INSERT INTO test.00745_merge_tree_map1 VALUES (2, {'k2': 'v2'}, {'k2': 'v2'});
SELECT n, m1{'k1'}, m2{'k1'} FROM test.00745_merge_tree_map1 ORDER BY n;

-- write parts to test.00745_merge_tree_map2
INSERT INTO test.00745_merge_tree_map2 VALUES (1, {'k1': 'v1'}, {'k1': 'v1'});
SELECT n, m1{'k1'}, m2{'k1'} FROM test.00745_merge_tree_map2;

INSERT INTO test.00745_merge_tree_map2 VALUES (2, {'k2': 'v2'}, {'k2': 'v2'});
SELECT n, m1{'k1'}, m2{'k1'} FROM test.00745_merge_tree_map2 ORDER BY n;

SELECT n, m1{'k2'}, m2{'k2'} FROM test.00745_merge_tree_map1 ORDER BY n;
SELECT n, m1{'k2'}, m2{'k2'} FROM test.00745_merge_tree_map2 ORDER BY n;

ALTER TABLE test.00745_merge_tree_map1 ADD COLUMN ma Map(String, Array(String));
ALTER TABLE test.00745_merge_tree_map2 ADD COLUMN ma Map(String, Array(String)) KV;

INSERT INTO test.00745_merge_tree_map1 VALUES (3, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': ['v3.1', 'v3.2']});
SELECT n, m1{'k3'}, m2{'k3'}, ma{'k3'} FROM test.00745_merge_tree_map1 ORDER BY n;

INSERT INTO test.00745_merge_tree_map2 VALUES (3, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': ['v3.1', 'v3.2']});
SELECT n, m1{'k3'}, m2{'k3'}, ma{'k3'} FROM test.00745_merge_tree_map2 ORDER BY n;

ALTER TABLE test.00745_merge_tree_map1 DROP COLUMN ma;
ALTER TABLE test.00745_merge_tree_map2 DROP COLUMN ma;

DESC TABLE test.00745_merge_tree_map1;
SELECT n, arraySort(mapKeys(m1)), arraySort(mapKeys(m2)) FROM test.00745_merge_tree_map1 ORDER BY n;

DESC TABLE test.00745_merge_tree_map2;
SELECT n, arraySort(mapKeys(m1)), arraySort(mapKeys(m2)) FROM test.00745_merge_tree_map2 ORDER BY n;

ALTER TABLE test.00745_merge_tree_map1 CLEAR MAP KEY m1('k2'), CLEAR MAP KEY m2('k2');
ALTER TABLE test.00745_merge_tree_map2 CLEAR MAP KEY m1('k2'), CLEAR MAP KEY m2('k2'); -- { serverError 44 }

SELECT n, m1, m2 FROM test.00745_merge_tree_map1 ORDER BY n;

DROP TABLE test.00745_merge_tree_map1;
DROP TABLE test.00745_merge_tree_map2;
