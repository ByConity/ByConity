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

select * from test_inverted_with_ngram_chinese where doc like '%查询%';

DROP TABLE IF EXISTS test_inverted_with_ngram_chinese;

CREATE TABLE test_inverted_with_ngram_chinese
(
    `key` UInt64,
    `doc` String,
    INDEX doc_idx doc TYPE inverted(2) GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key SETTINGS index_granularity = 1;

INSERT INTO test_inverted_with_ngram_chinese VALUES (0, 'ByConity 是分布式的云原生SQL数仓引擎'), (1, '擅长交互式查询和即席查询'), (2, '具有支持多表关联复杂查询'), (3, '集群扩容无感'), (4, '离线批数据和实时数据流统一汇总等特点。');

select * from test_inverted_with_ngram_chinese where doc like '%查询%';

DROP TABLE IF EXISTS test_inverted_with_ngram_chinese;



