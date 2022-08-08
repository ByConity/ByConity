DROP TABLE IF EXISTS test.interactive_txn_0;
DROP TABLE IF EXISTS test.interactive_txn_1;

SET enable_interactive_transaction=1;

--- TEST insert
SELECT 'INSERT TEST';

CREATE TABLE test.interactive_txn_0 (d String, id UInt64) ENGINE=CnchMergeTree() ORDER BY id PARTITION BY d;

INSERT INTO test.interactive_txn_0 VALUES ('interactive_txn_0', 42);

CREATE TABLE test.interactive_txn_1 (d String, id UInt64) ENGINE=CnchMergeTree() ORDER BY id PARTITION BY d;

SELECT * FROM test.interactive_txn_0 ORDER BY id;
SELECT * FROM test.interactive_txn_1 ORDER BY id;


SELECT 'Test 0: insert values rollback';

BEGIN;
INSERT INTO test.interactive_txn_0 VALUES ('interactive_txn_0', 1);
INSERT INTO test.interactive_txn_1 VALUES ('interactive_txn_1', 2);
SELECT * FROM test.interactive_txn_0 ORDER BY id;
SELECT * FROM test.interactive_txn_1 ORDER BY id;
ROLLBACK;

SELECT 'After rollback';
SELECT * FROM test.interactive_txn_0 ORDER BY id;
SELECT * FROM test.interactive_txn_1 ORDER BY id;

SELECT 'Test 1: insert values commit';

BEGIN;
INSERT INTO test.interactive_txn_0 VALUES ('interactive_txn_0', 3);
INSERT INTO test.interactive_txn_1 VALUES ('interactive_txn_1', 4);
SELECT * FROM test.interactive_txn_0 ORDER BY id;
SELECT * FROM test.interactive_txn_1 ORDER BY id;
COMMIT;

SELECT 'After commit';
SELECT * FROM test.interactive_txn_0 ORDER BY id;
SELECT * FROM test.interactive_txn_1 ORDER BY id;

SELECT 'Test 2: insert select rollback';

BEGIN;
INSERT INTO test.interactive_txn_0 VALUES ('interactive_txn_0', 5);
INSERT INTO test.interactive_txn_1 SELECT * FROM test.interactive_txn_0 ORDER BY id;
SELECT * FROM test.interactive_txn_0 ORDER BY id;
SELECT * FROM test.interactive_txn_1 ORDER BY id;
ROLLBACK;

SELECT 'After rollback';
SELECT * FROM test.interactive_txn_0 ORDER BY id;
SELECT * FROM test.interactive_txn_1 ORDER BY id;

SELECT 'Test 3: insert select commit';

BEGIN;
INSERT INTO test.interactive_txn_0 VALUES ('interactive_txn_0', 6);
INSERT INTO test.interactive_txn_1 SELECT * FROM test.interactive_txn_0 ORDER BY id;
SELECT * FROM test.interactive_txn_0 ORDER BY id;
SELECT * FROM test.interactive_txn_1 ORDER BY id;
COMMIT;

SELECT 'After commit';
SELECT * FROM test.interactive_txn_0 ORDER BY id;
SELECT * FROM test.interactive_txn_1 ORDER BY id;

--- TEST truncate
SELECT 'TRUNCATE TEST';
DROP TABLE IF EXISTS test.interactive_txn_2;
CREATE TABLE test.interactive_txn_2 as test.interactive_txn_1;
INSERT INTO test.interactive_txn_2 VALUES ('interactive_txn_2_1', 42);
INSERT INTO test.interactive_txn_2 VALUES ('interactive_txn_2_2', 43);

SELECT * FROM test.interactive_txn_2 ORDER BY id;

SELECT 'Test 4: truncate rollback';
BEGIN;
TRUNCATE TABLE test.interactive_txn_2;
SELECT * FROM test.interactive_txn_2 ORDER BY id;
ROLLBACK;

SELECT 'After rollback';
SELECT * FROM test.interactive_txn_2 ORDER BY id;

SELECT 'Test 5: truncate commit';
BEGIN;
TRUNCATE TABLE test.interactive_txn_2;
COMMIT;

SELECT 'After commit';
SELECT * FROM test.interactive_txn_2 ORDER BY id;

DROP TABLE IF EXISTS test.interactive_txn_0;
DROP TABLE IF EXISTS test.interactive_txn_1;
DROP TABLE IF EXISTS test.interactive_txn_2;