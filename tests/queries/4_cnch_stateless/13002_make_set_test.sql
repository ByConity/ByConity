SELECT make_set(1, 'hello', 'world');
SELECT make_set(5, 'hello', 'nice', 'world');
SELECT make_set(5, 'hello', 'nice', NULL, 'world');
SELECT make_set(7, 'hello', 'nice', 'world');
SELECT make_set(0, 'hello', 'world');
SELECT make_set(NULL, 'hello', 'world');
SELECT make_set(-1, 'hello', 'world');
SELECT make_set(-1, 'hello', 1.2345);
SELECT make_set(-1, 'hello', 1.2345, NULL);
SELECT make_set(-1, 'hello', NULL, 1.2345);
SELECT make_set(-1, 5, 1.2345, NULL);
SELECT make_set(-1, -21, 1.2345, NULL);

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test_make_set;
CREATE TABLE test_make_set (
  id Int64,
  bits Int64,
  str1 String,
  str2 String,
  str3 Nullable(String),
  num Float32
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO test_make_set (id, bits, str1, str2, str3, num) VALUES (0, 1, 'hello', 'world', 'foo', 1.232), (1, 5, 'hello', 'nice', 'world', 2.323), (2, 6, 'nice', 'world', NULL, 3.4332), (3, 0, '', 'foo', 'bar', 4.5123), (4, -1, 'hello', 'nice', 'world', 5.978), (5, 0, 'hello', 'world', NULL, 69954), (6, NULL, 'hello', 'world', NULL, 7328.432), (7, 7, 'hello', 'world', NULL, 0), (8, 7, 'hello', 1.2345, NULL, 0);

SELECT id, make_set(bits, str1, str2, str3, num) FROM test_make_set ORDER BY id;
DROP TABLE IF EXISTS test_make_set;
