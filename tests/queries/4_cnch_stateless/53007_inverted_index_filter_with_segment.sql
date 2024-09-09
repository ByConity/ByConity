DROP TABLE IF EXISTS test_inverted_with_token sync;

set filter_with_inverted_index_segment = 1;

select 'test1';

CREATE TABLE test_inverted_with_token
(
    `key` UInt64,
    `doc` String,
    INDEX doc_idx lower(doc) TYPE inverted GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key;

INSERT INTO test_inverted_with_token VALUES (0, 'Sail against the wind'), (1, 'Wait and see'), (2, 'Sail the seven seas'),(3, 'See how the wind blows');

select * from test_inverted_with_token where lower(doc) like '%wind%';
select * from test_inverted_with_token where hasToken(lower(doc), 'wind');

DROP TABLE IF EXISTS test_inverted_with_token sync;

select 'test2';

DROP TABLE IF EXISTS test_inverted_with_ngram sync;

CREATE TABLE test_inverted_with_ngram
(
    `key` UInt64,
    `doc` String,
    INDEX doc_idx lower(doc) TYPE inverted(4) GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key;

INSERT INTO test_inverted_with_ngram VALUES (0, 'Sail against the wind'), (1, 'Wait and see'), (2, 'Sail the seven seas'),(3, 'See how the wind blows');

select * from test_inverted_with_ngram where lower(doc) like '%wind%';
select * from test_inverted_with_ngram where hasToken(lower(doc), 'wind');

DROP TABLE IF EXISTS test_inverted_with_ngram sync;


-- test with index_granularity = 1 ;

select 'test3';

DROP TABLE IF EXISTS test_inverted_with_token sync;

CREATE TABLE test_inverted_with_token
(
    `key` UInt64,
    `doc` String,
    INDEX doc_idx lower(doc) TYPE inverted GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key SETTINGS index_granularity = 1;

INSERT INTO test_inverted_with_token VALUES (0, 'Sail against the wind'), (1, 'Wait and see'), (2, 'Sail the seven seas'),(3, 'See how the wind blows');

select * from test_inverted_with_token where lower(doc) like '%wind%';
select * from test_inverted_with_token where hasToken(lower(doc), 'wind');

DROP TABLE IF EXISTS test_inverted_with_token sync;

select 'test4';

DROP TABLE IF EXISTS test_inverted_with_ngram sync;

CREATE TABLE test_inverted_with_ngram
(
    `key` UInt64,
    `doc` String,
    INDEX doc_idx lower(doc) TYPE inverted(4) GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key SETTINGS index_granularity = 1;

INSERT INTO test_inverted_with_ngram VALUES (0, 'Sail against the wind'), (1, 'Wait and see'), (2, 'Sail the seven seas'),(3, 'See how the wind blows');

select * from test_inverted_with_ngram where lower(doc) like '%wind%';
select * from test_inverted_with_ngram where hasToken(lower(doc), 'wind');

DROP TABLE IF EXISTS test_inverted_with_ngram sync;
