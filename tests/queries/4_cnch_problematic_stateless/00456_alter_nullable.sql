SET allow_alter_with_unfinished_task = 1;

DROP TABLE IF EXISTS test.nullable_alter;
CREATE TABLE test.nullable_alter (d Date DEFAULT '2000-01-01', x String) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY d SETTINGS index_granularity = 1;

INSERT INTO test.nullable_alter (x) VALUES ('Hello'), ('World');
SELECT x FROM test.nullable_alter ORDER BY x;

ALTER TABLE test.nullable_alter MODIFY COLUMN x Nullable(String);
SELECT x FROM test.nullable_alter ORDER BY x;

INSERT INTO test.nullable_alter (x) VALUES ('xyz'), (NULL);
SELECT x FROM test.nullable_alter ORDER BY x NULLS FIRST;

ALTER TABLE test.nullable_alter MODIFY COLUMN x Nullable(FixedString(5));
SELECT x FROM test.nullable_alter ORDER BY x NULLS FIRST;

DROP TABLE test.nullable_alter;
