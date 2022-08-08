DROP TABLE IF EXISTS test.default;

CREATE TABLE test.default (d Date DEFAULT toDate(t), t DateTime) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY t;
INSERT INTO test.default (t) VALUES ('1234567890');
SELECT toStartOfMonth(d), toUInt32(t) FROM test.default;

DROP TABLE test.default;
