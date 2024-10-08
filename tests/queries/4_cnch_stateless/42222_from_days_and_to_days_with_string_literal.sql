SELECT from_days('0'), from_days('2147483647'), from_days('719528'), from_days(CAST(1000000000 AS Int64)), from_days(NULL);
SELECT to_days('1900-01-01'), to_days('1970-03-13'), to_days('2100-01-01 19:02:34'), to_days('2299-12-31 12:32:34.234125'), to_days(NULL);

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test_from_days;
CREATE TABLE test_from_days (
    id Int64,
    str String
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO test_from_days VALUES (0, '0'), (1, '2147483647'), (2, '719528');

SELECT id, from_days(str) FROM test_from_days ORDER BY id;

DROP TABLE IF EXISTS test_to_days;
CREATE TABLE test_to_days (
    id Int64,
    str String
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO test_to_days VALUES (0, '1900-01-01'), (1, '1970-03-13'), (2, '2100-01-01 19:02:34'), (3, '2299-12-31 12:32:34.234125');

SELECT id, to_days(str) FROM test_to_days ORDER BY id;

DROP TABLE IF EXISTS test_from_days;
DROP TABLE IF EXISTS test_to_days;