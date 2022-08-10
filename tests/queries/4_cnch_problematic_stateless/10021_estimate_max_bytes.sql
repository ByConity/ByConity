DROP TABLE IF EXISTS test.estimate_max_bytes;
CREATE TABLE test.estimate_max_bytes (id Int64, name String) Engine = CnchMergeTree Order by (id) SETTINGS index_granularity = 2;

INSERT INTO test.estimate_max_bytes VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

SELECT sum(id) FROM test.estimate_max_bytes;
SELECT sum(id), count(name) FROM test.estimate_max_bytes;

SET max_compressed_bytes_to_read=30;
SELECT sum(id) FROM test.estimate_max_bytes; -- {serverError 307}
SELECT sum(id) FROM test.estimate_max_bytes WHERE id < 3;

SET max_compressed_bytes_to_read=50;
SELECT sum(id) FROM test.estimate_max_bytes;
SELECT sum(id), count(name) FROM test.estimate_max_bytes; -- {serverError 307}

SET max_compressed_bytes_to_read=0;
SELECT sum(id) FROM test.estimate_max_bytes;
SELECT sum(id), count(name) FROM test.estimate_max_bytes;

SET max_uncompressed_bytes_to_read=20;
SELECT sum(id) FROM test.estimate_max_bytes; -- {serverError 307}
SELECT sum(id) FROM test.estimate_max_bytes WHERE id < 3;

SET max_uncompressed_bytes_to_read=35;
SELECT sum(id) FROM test.estimate_max_bytes;
SELECT sum(id), count(name) FROM test.estimate_max_bytes; -- {serverError 307}

SET max_uncompressed_bytes_to_read=0;
SELECT sum(id) FROM test.estimate_max_bytes;
SELECT sum(id), count(name) FROM test.estimate_max_bytes;

SET max_compressed_bytes_to_read=100;
SET max_uncompressed_bytes_to_read=80;
SELECT sum(id) FROM test.estimate_max_bytes;
SELECT sum(id), count(name) FROM test.estimate_max_bytes;

DROP TABLE test.estimate_max_bytes;
