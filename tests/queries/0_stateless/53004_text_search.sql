USE test;

SELECT toTextSearchQuery('(!!hello | world )& 世界 | !( SQL | ok  ) & yes | 你好');
SELECT toTextSearchQuery('(!!hello | world )    &  世界 |  !( SQL |   ok    ) &  yes |        你好');
SELECT toTextSearchQuery('(!!😁 | 😁 )    &  😁 |  !( SQL |   😈  ) &  yes |        你好');

DROP TABLE IF EXISTS test_inverted_with_ngram_chinese;

CREATE TABLE test_inverted_with_ngram_chinese
(
    `key` UInt64,
    `doc` String,
    INDEX doc_idx doc TYPE inverted(2) GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key;

INSERT INTO test_inverted_with_ngram_chinese VALUES (0, 'ByConity 是分布式的云原生SQL数仓引擎'), (1, '擅长交互式查询和即席查询'), (2, '具有支持多表关联复杂查询'), (3, '集群扩容无感'), (4, '离线批数据和实时数据流统一汇总等特点。');

select * from test_inverted_with_ngram_chinese where textSearch(doc, '查询');
select * from test_inverted_with_ngram_chinese where textSearch(doc, '查询 | 数仓');
select * from test_inverted_with_ngram_chinese where textSearch(doc, '查询 & 即席');
select * from test_inverted_with_ngram_chinese where textSearch(doc, '查询 & !即席');
select * from test_inverted_with_ngram_chinese where textSearch(doc, '查询 & !复杂 &  ( 交互 | 擅长) | 数仓 & !数仓');


DROP TABLE IF EXISTS test_inverted_with_ngram_chinese;

CREATE TABLE test_inverted_with_ngram_chinese
(
    `key` UInt64,
    `doc` String,
    INDEX doc_idx doc TYPE inverted(2) GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key  SETTINGS index_granularity = 1;

INSERT INTO test_inverted_with_ngram_chinese VALUES (0, 'ByConity 是分布式的云原生SQL数仓引擎'), (1, '擅长交互式查询和即席查询'), (2, '具有支持多表关联复杂查询'), (3, '集群扩容无感'), (4, '离线批数据和实时数据流统一汇总等特点。');

select * from test_inverted_with_ngram_chinese where textSearch(doc, '查询');
select * from test_inverted_with_ngram_chinese where textSearch(doc, '查询 | 数仓');
select * from test_inverted_with_ngram_chinese where textSearch(doc, '查询 & 即席');
select * from test_inverted_with_ngram_chinese where textSearch(doc, '查询 & !即席');
select * from test_inverted_with_ngram_chinese where textSearch(doc, '查询 & ! 复杂 &  ( 交互 | 擅长) | 数仓 & !数仓');

DROP TABLE IF EXISTS test_inverted_with_ngram_chinese;
