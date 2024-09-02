drop table if exists test_ingest_local;
drop table if exists test_ingest_source_local;

CREATE TABLE test_ingest_local
(
    `p_date` Date,
    `id` Int32,
    `key` Int32
)
ENGINE = CnchMergeTree
PARTITION BY p_date
ORDER BY (id, cityHash64(id)) SETTINGS index_granularity = 8192;

SYSTEM START MERGES test_ingest_local;

CREATE TABLE test_ingest_source_local
(
    `p_date` Date,
    `id` Int32,
    `key` Int32
)
ENGINE = CnchMergeTree
PARTITION BY p_date
ORDER BY (id, cityHash64(id)) SETTINGS index_granularity = 8192;

insert into test_ingest_local select '2024-01-01', number, number from numbers(10);
insert into test_ingest_source_local select '2024-01-01', number, number from numbers(10);
alter table test_ingest_local ingest partition id '20240101' columns key from test_ingest_source_local;

drop table if exists test_ingest_local;
drop table if exists test_ingest_source_local;