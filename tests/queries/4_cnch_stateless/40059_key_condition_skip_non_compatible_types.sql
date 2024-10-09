DROP TABLE IF EXISTS test_dec;

CREATE TABLE test_dec (`key` Decimal(20, 10), `val` Decimal(20, 10)) ENGINE = CnchMergeTree ORDER BY key;

INSERT INTO test_dec VALUES (0, 0) (0.09, 0.09) (0.11, 0.11) (1.0, 1.0);
select * from test_dec where key > 0.1 ORDER BY key;

DROP TABLE IF EXISTS test_dec;
