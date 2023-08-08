DROP TABLE IF EXISTS test_float_pk;
CREATE TABLE test_float_pk (`x` Float32) ENGINE = CnchMergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO test_float_pk VALUES (1) (1.5) (1.7) (2);
SELECT x FROM test_float_pk WHERE x > 1;
SELECT x FROM test_float_pk WHERE x > 1.1;

DROP TABLE IF EXISTS test_nullable_float_pk;
CREATE TABLE test_nullable_float_pk (`x` Nullable(Float32)) ENGINE = CnchMergeTree ORDER BY x SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO test_nullable_float_pk VALUES (1) (1.5) (1.7) (2) (NULL);
SELECT x FROM test_nullable_float_pk WHERE x > 1;
SELECT x FROM test_nullable_float_pk WHERE x > 1.1;

DROP TABLE IF EXISTS test_float64_pk;
CREATE TABLE test_float64_pk (`x` Float64) ENGINE = CnchMergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO test_float64_pk VALUES (1) (1.5) (1.7) (2);
SELECT x FROM test_float64_pk WHERE x > 1;
SELECT x FROM test_float_pk WHERE x > 1.1;


DROP TABLE IF EXISTS test_nullable_float64_pk;
CREATE TABLE test_nullable_float64_pk (`x` Nullable(Float64)) ENGINE = CnchMergeTree ORDER BY x SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO test_nullable_float64_pk VALUES (1) (1.5) (1.7) (2) (NULL);
SELECT x FROM test_nullable_float64_pk WHERE x > 1;
SELECT x FROM test_nullable_float64_pk WHERE x > 1.1;
