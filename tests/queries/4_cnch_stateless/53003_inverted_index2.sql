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

DROP TABLE IF EXISTS ivt_v0_store;
CREATE TABLE ivt_v0_store
(
    `doc` String,
    INDEX doc_idx doc TYPE inverted('standard', '{"version": "v0"}') GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO ivt_v0_store VALUES ('ByConity 是分布式的 云原生 SQL 数仓引擎'), ('擅长 交互式 查询 和 即席查询'), ('具有 支持 多表关联 复杂 查询'), ('集群 扩容 无感'), ('离线 批数据 和 实时数据流 统一 汇总 等 特点。');

select 'ivt_v0_store query';
select * from ivt_v0_store where doc like '%查询%';

DROP TABLE IF EXISTS ivt_v2_store;
CREATE TABLE ivt_v2_store
(
    `doc` String,
    INDEX doc_idx doc TYPE inverted('standard', '{"version": "v2"}') GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO ivt_v2_store VALUES ('ByConity 是分布式的 云原生 SQL 数仓引擎'), ('擅长 交互式 查询 和 即席查询'), ('具有 支持 多表关联 复杂 查询'), ('集群 扩容 无感'), ('离线 批数据 和 实时数据流 统一 汇总 等 特点。');

select 'ivt_v2_store query';
select * from ivt_v2_store where doc like '%查询%';
