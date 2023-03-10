
DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    dt Date,
    id UInt32,
    val Nullable(UInt32)
)
ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id SETTINGS index_granularity = 8192;

insert into test (dt, id, val) values ('2017-01-01', 1, 10);
insert into test (dt, id, val) values ('2017-01-01', 1, null);
insert into test (dt, id, val) values ('2017-01-01', 1, 0);

SELECT count()
FROM test
WHERE val = 0;

DROP TABLE IF EXISTS test;
