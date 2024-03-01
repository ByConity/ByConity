USE test;
DROP TABLE IF EXISTS test.enum_nested_alter;
CREATE TABLE test.enum_nested_alter(d Date DEFAULT '2000-01-01', x UInt64, n Nested(a String, e Enum8('Hello' = 1), b UInt8)) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY x SETTINGS index_granularity=1;

INSERT INTO test.enum_nested_alter (x, n.e) VALUES (1, ['Hello']);
SELECT * FROM test.enum_nested_alter;

ALTER TABLE test.enum_nested_alter MODIFY COLUMN n.e Array(Enum8('Hello' = 1, 'World' = 2));
-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

INSERT INTO test.enum_nested_alter (x, n.e) VALUES (2, ['World']);
SELECT * FROM test.enum_nested_alter ORDER BY x;

ALTER TABLE test.enum_nested_alter MODIFY COLUMN n.e Array(Enum16('Hello' = 1, 'World' = 2, 'a' = 300));
-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

SELECT * FROM test.enum_nested_alter ORDER BY x;

ALTER TABLE test.enum_nested_alter MODIFY COLUMN n.e Array(Int16);
-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

SELECT * FROM test.enum_nested_alter ORDER BY x;

DROP TABLE test.enum_nested_alter;
CREATE TABLE test.enum_nested_alter(n Nested(a Int16, b Enum16('Hello' = 1, 'World' = 2, 'a' = 300), c String)) Engine = CnchMergeTree() ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO test.enum_nested_alter VALUES ([1, 2], ['Hello', 'World'], ['Hello', 'World']), ([1, 2], ['Hello', 'a'], ['World', 'a']);

ALTER TABLE test.enum_nested_alter MODIFY COLUMN n.a Array(Enum16('Hello' = 1, 'World' = 2, 'a' = 300));
ALTER TABLE test.enum_nested_alter MODIFY COLUMN n.b Array(String);
ALTER TABLE test.enum_nested_alter MODIFY COLUMN n.c Array(Enum16('Hello' = 1, 'World' = 2, 'a' = 300));

SELECT toTypeName(n.a), toTypeName(n.b), toTypeName(n.c) FROM test.enum_nested_alter LIMIT 1;
SELECT * FROM test.enum_nested_alter;

DROP TABLE test.enum_nested_alter;


CREATE TABLE test.enum_nested_alter
(
    d Date DEFAULT '2000-01-01', 
    x UInt64, 
    tasks Nested(
        errcategory Enum8(
            'undefined' = 0, 'system' = 1, 'generic' = 2, 'asio.netdb' = 3, 'asio.misc' = 4, 
            'asio.addrinfo' = 5, 'rtb.client' = 6, 'rtb.logic' = 7, 'http.status' = 8), 
        status Enum16('hello' = 1, 'world' = 2)))
ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY x SETTINGS index_granularity=1;

INSERT INTO test.enum_nested_alter (x, tasks.errcategory, tasks.status) VALUES (1, ['system', 'rtb.client'], ['hello', 'world']);
SELECT * FROM test.enum_nested_alter ORDER BY x;

ALTER TABLE test.enum_nested_alter 
    MODIFY COLUMN tasks.errcategory Array(Enum8(
            'undefined' = 0, 'system' = 1, 'generic' = 2, 'asio.netdb' = 3, 'asio.misc' = 4, 
            'asio.addrinfo' = 5, 'rtb.client' = 6, 'rtb.logic' = 7, 'http.status' = 8, 'http.code' = 9)),
    MODIFY COLUMN tasks.status Array(Enum8('hello' = 1, 'world' = 2, 'goodbye' = 3));

INSERT INTO test.enum_nested_alter (x, tasks.errcategory, tasks.status) VALUES (2, ['http.status', 'http.code'], ['hello', 'goodbye']);
SELECT * FROM test.enum_nested_alter ORDER BY x;

DROP TABLE test.enum_nested_alter;


DROP TABLE IF EXISTS test.enum_nested_alter;
CREATE TABLE test.enum_nested_alter(d Date DEFAULT '2000-01-01', x UInt64, n Nested(a String, e Enum8('Hello.world' = 1), b UInt8)) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY x SETTINGS index_granularity = 1;

INSERT INTO test.enum_nested_alter (x, n.e) VALUES (1, ['Hello.world']);
SELECT * FROM test.enum_nested_alter;

ALTER TABLE test.enum_nested_alter MODIFY COLUMN n.e Array(Enum8('Hello.world' = 1, 'a' = 2));
-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

SELECT * FROM test.enum_nested_alter;

DROP TABLE test.enum_nested_alter;
