USE test;

DROP TABLE IF EXISTS test_inverted_with_arry_str;

CREATE TABLE test_inverted_with_arry_str
(
    `id` UInt64,
    `doc` Array(String),
    INDEX doc_idx doc TYPE inverted() GRANULARITY 1 
)
ENGINE = CnchMergeTree()
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_inverted_with_arry_str VALUES (0, ['你好','世界']);
INSERT INTO test_inverted_with_arry_str VALUES (1, ['Hello','World']);
INSERT INTO test_inverted_with_arry_str VALUES (2, ['Hello','世界']);
INSERT INTO test_inverted_with_arry_str VALUES (3, ['你好','World']);

-- test has
SELECT * FROM test_inverted_with_arry_str WHERE has(doc, '你好') ORDER BY id;
SELECT * FROM test_inverted_with_arry_str WHERE has(doc, 'World') ORDER BY id;
SELECT * FROM test_inverted_with_arry_str WHERE has(doc, 'HELLO') ORDER BY id;
SELECT * FROM test_inverted_with_arry_str WHERE has(doc, '你好啊') ORDER BY id;

-- test hasAny
SELECT * FROM test_inverted_with_arry_str WHERE hasAny(doc, ['你好啊','Hello']) ORDER BY id;
SELECT * FROM test_inverted_with_arry_str WHERE hasAny(doc, ['你好','HELLO']) ORDER BY id;
SELECT * FROM test_inverted_with_arry_str WHERE hasAny(doc, ['世界','HELLO']) ORDER BY id;
SELECT * FROM test_inverted_with_arry_str WHERE hasAny(doc, ['HELLO','WORLD']) ORDER BY id;

-- test arraySetCheck
SELECT * FROM test_inverted_with_arry_str WHERE arraySetCheck(doc, ['你好啊','Hello']) ORDER BY id;
SELECT * FROM test_inverted_with_arry_str WHERE arraySetCheck(doc, ['你好','HELLO']) ORDER BY id;
SELECT * FROM test_inverted_with_arry_str WHERE arraySetCheck(doc, ['世界','HELLO']) ORDER BY id;
SELECT * FROM test_inverted_with_arry_str WHERE arraySetCheck(doc, ['HELLO','WORLD']) ORDER BY id;

DROP TABLE IF EXISTS test_inverted_with_arry_str;