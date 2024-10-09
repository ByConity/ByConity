DROP TABLE IF EXISTS test_inverted_with_token;

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

DROP TABLE IF EXISTS test_inverted_with_token;



DROP TABLE IF EXISTS test_inverted_with_ngram;

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

DROP TABLE IF EXISTS test_inverted_with_ngram;


-- test with index_granularity = 1 ;

DROP TABLE IF EXISTS test_inverted_with_token;

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

DROP TABLE IF EXISTS test_inverted_with_token;



DROP TABLE IF EXISTS test_inverted_with_ngram;

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

DROP TABLE IF EXISTS test_inverted_with_ngram;
    