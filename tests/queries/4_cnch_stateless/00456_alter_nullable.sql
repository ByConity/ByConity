
DROP TABLE IF EXISTS nullable_alter;
CREATE TABLE nullable_alter (d Date DEFAULT '2000-01-01', x String) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY d SETTINGS index_granularity = 1;

INSERT INTO nullable_alter (x) VALUES ('Hello'), ('World');
SELECT x FROM nullable_alter ORDER BY x;

-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

ALTER TABLE nullable_alter MODIFY COLUMN x Nullable(String);
SELECT x FROM nullable_alter ORDER BY x;

INSERT INTO nullable_alter (x) VALUES ('xyz'), (NULL);
SELECT x FROM nullable_alter ORDER BY x NULLS FIRST;

-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

ALTER TABLE nullable_alter MODIFY COLUMN x Nullable(FixedString(5));
SELECT x FROM nullable_alter ORDER BY x NULLS FIRST;

DROP TABLE nullable_alter;
