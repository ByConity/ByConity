DROP TABLE IF EXISTS test_inverted_with_standard;

CREATE TABLE test_inverted_with_standard
(
    `key` UInt64,
    `doc` String,
    INDEX inv_stand `doc` TYPE inverted('standard', '{}') GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key SETTINGS index_granularity = 1;

INSERT INTO test_inverted_with_standard VALUES (0, 'ByConity 是分布式的云原生SQL数仓引擎'), (1, '擅长交互式查询和即席查询'), (2, '具有支持多表关联复杂查询'), (3, '集群扩容无感'), (4, '离线批数据和实时数据流统一汇总等特点。');

select * from test_inverted_with_standard where doc like '%查询%';
select * from test_inverted_with_standard where hasTokens(doc, '查询');
select 'with disable has result';
select * from test_inverted_with_standard where hasTokens(doc, 'Con') SETTINGS enable_inverted_index = 0;
select 'with enable not has result';
select * from test_inverted_with_standard where hasTokens(doc, 'Con');

DROP TABLE IF EXISTS test_inverted_with_standard;