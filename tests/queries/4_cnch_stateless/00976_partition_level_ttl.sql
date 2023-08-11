DROP TABLE IF EXISTS ttl_test;

CREATE TABLE ttl_test (ts DateTime, id Int32, s String) ENGINE=CnchMergeTree PARTITION BY toDate(ts) ORDER BY id;

INSERT INTO ttl_test VALUES ('2022-01-01 12:00:00', 1001, 'test1');
INSERT INTO ttl_test VALUES ('2022-01-02 12:00:00', 1002, 'test2');

SELECT * FROM ttl_test ORDER BY id;

ALTER TABLE ttl_test MODIFY TTL toDate(ts) + INTERVAL 7 DAY;

SELECT * FROM ttl_test ORDER BY id;
