-- hasTokens() is same with like '%%' without inverted index filter
-- test hasTokens with true
SELECT hasTokens('abcd-efg-hi-jk-lm-n-opqrst-uvwxyz','hi-jk');
SELECT hasTokens('abcd-efg-hi-jk-lm-n-opqrst-uvwxyz','abcd');
SELECT hasTokens('abcd-efg-hi-jk-lm-n-opqrst-uvwxyz','uvwxyz');
SELECT hasTokens('abcd-efg-hi-jk-lm--n--opqrst-uvwxyz','n');
SELECT hasTokens('abcd-efg-hi-jk-lm--n--opqrst-uvwxyz','-n-');
SELECT hasTokens('abcd-efg-hi-jk-lm-n-opqrst-uvwxyz','-hi-');
SELECT hasTokens('abcd-efg-hi-jk-lm-n-opqrst-uvwxyz','xyz');
SELECT hasTokens('abcd-efg-hi-jk-lm-n-opqrst-uvwxyz','-hi-jk-');
SELECT hasTokens('abc你好ABC','你好');
SELECT hasTokens('abc你好ABC','好');


-- test hasTokens with  token inverted index

drop table if exists test_inverted_with_token;

CREATE TABLE test_inverted_with_token
(
    `key` UInt64,
    `doc` String,
    INDEX doc_idx doc TYPE inverted GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key
settings index_granularity = 1;

INSERT INTO test_inverted_with_token VALUES (0, 'begin-first-second-thrid-end'), (1, 'begin-second-first-thrid-end'), (2, 'end-begin'), (3, 'begin-second-thrid-end'), (4, 'abc你好ABC');

-- 1
select 'true';
select '-----';
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, 'first-second-thrid');
select '-----';
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, 'end-begin');
select '-----';
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, 'first');
select '-----';
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, 'begin');

-- 0
select 'false';
select 'without index';
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, 'st-second-th') settings enable_inverted_index  = 0;
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, '你好') settings enable_inverted_index  = 0;
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, 'begin-first-') settings enable_inverted_index  = 0;
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, '-first-second-') settings enable_inverted_index  = 0;
select 'use index';
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, 'st-second-th');
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, '你好');
select 'has value';
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, 'begin-first-');
SELECT * FROM test_inverted_with_token WHERE hasTokens(doc, '-first-second-');


drop table if exists test_inverted_with_token;

drop table if exists test_inverted_with_ngram;

-- test hasTokens with ngram inverted index
CREATE TABLE test_inverted_with_ngram
(
    `key` UInt64,
    `doc` String,
    INDEX doc_idx doc TYPE inverted(2) GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key
settings index_granularity = 1;

INSERT INTO test_inverted_with_ngram VALUES (0, 'begin-first-second-thrid-end'), (1, 'begin-second-first-thrid-end'), (2, 'end-begin'), (3, 'begin-second-thrid-end'), (4, 'abc你好ABC');

-- 1
select 'true';
select '-----';
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, 'first-second-thrid');
select '-----';
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, 'end-begin');
select '-----';
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, 'first');
select '-----';
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, 'begin');

-- 0
select 'false';
select 'without index';
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, 'st-second-th') settings enable_inverted_index = 0;
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, 'begin-first-') settings enable_inverted_index = 0;
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, '-first-second-') settings enable_inverted_index = 0;
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, '你好') settings enable_inverted_index = 0;
select 'use index';
select 'has value';
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, 'st-second-th');
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, 'begin-first-');
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, '-first-second-');
SELECT * FROM test_inverted_with_ngram WHERE hasTokens(doc, '你好');

drop table if exists test_inverted_with_ngram;

