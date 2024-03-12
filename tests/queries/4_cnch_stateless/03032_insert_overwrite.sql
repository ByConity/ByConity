DROP TABLE IF EXISTS test_insert_overwrite_s;
DROP TABLE IF EXISTS test_insert_overwrite_t;

CREATE TABLE test_insert_overwrite_s (d Int32, n Int32) Engine = CnchMergeTree Partition by d order by n;
CREATE TABLE test_insert_overwrite_t (d Int32, n Int32) Engine = CnchMergeTree Partition by d order by n;

INSERT INTO test_insert_overwrite_s VALUES (1,1), (2,2);
INSERT INTO test_insert_overwrite_t VALUES (1,3), (2,4);

SELECT '----- Single partition insert overwrite select';

INSERT OVERWRITE test_insert_overwrite_t PARTITION (1) SELECT * FROM test_insert_overwrite_s WHERE d = 1;

SELECT * FROM test_insert_overwrite_t ORDER BY n;

SELECT '----- Single partition insert overwrite values';

INSERT OVERWRITE test_insert_overwrite_t PARTITION (2) VALUES (2,6);

SELECT * FROM test_insert_overwrite_t ORDER BY n;

SELECT '----- Multiple partitions insert overwrite select';

INSERT INTO test_insert_overwrite_s VALUES (1,11), (2,22);
INSERT OVERWRITE test_insert_overwrite_t PARTITION (1,2) SELECT * FROM test_insert_overwrite_s;

SELECT * FROM test_insert_overwrite_t ORDER BY n;

SELECT '----- Non-exist partition insert overwrite select';

INSERT INTO test_insert_overwrite_s VALUES (3, 33);
INSERT OVERWRITE test_insert_overwrite_t PARTITION (3) SELECT * FROM test_insert_overwrite_s WHERE d = 3;

SELECT * FROM test_insert_overwrite_t ORDER BY n;

SELECT '----- Single partition insert overwrite select with illegal partition';

TRUNCATE TABLE test_insert_overwrite_s;
TRUNCATE TABLE test_insert_overwrite_t;
INSERT INTO test_insert_overwrite_s VALUES (1,1), (2,2), (3,6), (4,8);
INSERT INTO test_insert_overwrite_t VALUES (1,3), (2,4);

INSERT OVERWRITE test_insert_overwrite_t PARTITION (1) SELECT * FROM test_insert_overwrite_s;

SELECT * FROM test_insert_overwrite_t ORDER BY n;

SELECT '----- Single partition insert overwrite values';

INSERT OVERWRITE test_insert_overwrite_t PARTITION (2) VALUES (2,6), (3,3);

SELECT * FROM test_insert_overwrite_t ORDER BY n;

SELECT '----- Multiple partitions insert overwrite select with illegal partition';

INSERT OVERWRITE test_insert_overwrite_t PARTITION ((2), (3)) SELECT * FROM test_insert_overwrite_s;

SELECT * FROM test_insert_overwrite_t ORDER BY n;

SELECT '----- Multiple partitions insert overwrite select with illegal partition';

INSERT OVERWRITE test_insert_overwrite_t PARTITION ((2), (3)) VALUES (2,4), (3,3), (4,4);

SELECT * FROM test_insert_overwrite_t ORDER BY n;

SELECT '----- Insert overwrite whole table select';

INSERT OVERWRITE test_insert_overwrite_t SELECT * FROM test_insert_overwrite_s;

SELECT * FROM test_insert_overwrite_t ORDER BY n;

SELECT '----- Insert overwrite whole table values';

INSERT OVERWRITE test_insert_overwrite_t VALUES (1, 11), (2, 22);

SELECT * FROM test_insert_overwrite_t ORDER BY n;

DROP TABLE test_insert_overwrite_s;
DROP TABLE test_insert_overwrite_t;

SELECT '----- Multiple partition by columns insert overwrite select with illegal partition';

CREATE TABLE test_insert_overwrite_s (s String, d Int32, n Int32) Engine = CnchMergeTree Partition by (s, d) order by n;
CREATE TABLE test_insert_overwrite_t (s String, d Int32, n Int32) Engine = CnchMergeTree Partition by (s, d) order by n;

INSERT INTO test_insert_overwrite_s VALUES ('1',1,1), ('2',2,2), ('3',3,3);
INSERT INTO test_insert_overwrite_t VALUES ('1',1,3), ('2',2,4);

INSERT OVERWRITE test_insert_overwrite_t PARTITION (('1',1), ('2',2)) SELECT * FROM test_insert_overwrite_s;

SELECT * FROM test_insert_overwrite_t ORDER BY n;

DROP TABLE test_insert_overwrite_s;
DROP TABLE test_insert_overwrite_t;

SELECT '----- Multiple partition by columns insert overwrite select with illegal partition for unique table';

CREATE TABLE test_insert_overwrite_s (s String, d Int32, n Int32) Engine = CnchMergeTree Partition by (s, d) unique key (n) order by n;
CREATE TABLE test_insert_overwrite_t (s String, d Int32, n Int32) Engine = CnchMergeTree Partition by (s, d) unique key (n) order by n;

INSERT INTO test_insert_overwrite_s VALUES ('1',1,1), ('2',2,2), ('3',3,3);
INSERT INTO test_insert_overwrite_t VALUES ('1',1,3), ('2',2,4);

INSERT OVERWRITE test_insert_overwrite_t PARTITION (('1',1), ('2',2)) SELECT * FROM test_insert_overwrite_s;

SELECT * FROM test_insert_overwrite_t ORDER BY n;

DROP TABLE test_insert_overwrite_s;
DROP TABLE test_insert_overwrite_t;

SELECT '----- Multiple partition by columns insert overwrite select with illegal partition and specified columns';

CREATE TABLE test_insert_overwrite_s (s String, d Int32, n Int32, m Int32) Engine = CnchMergeTree Partition by (s, d) order by n;
CREATE TABLE test_insert_overwrite_t (s String, d Int32, n Int32, m Int32) Engine = CnchMergeTree Partition by (s, d) order by n;

INSERT INTO test_insert_overwrite_s VALUES ('1',1,1,1), ('2',2,2,2), ('3',3,3,3);
INSERT INTO test_insert_overwrite_t VALUES ('1',1,3,3), ('2',2,4,4);

INSERT OVERWRITE test_insert_overwrite_t PARTITION (('1',1)) (s,d,n) SELECT s,d,n FROM test_insert_overwrite_s;

SELECT * FROM test_insert_overwrite_t ORDER BY n;

DROP TABLE test_insert_overwrite_s;
DROP TABLE test_insert_overwrite_t;
