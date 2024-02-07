drop table if exists test_nullable_skipindex sync;

CREATE TABLE test_nullable_skipindex
(
    `id` UInt64,
    `key_i` Nullable(UInt64),
    `key_f` Nullable(Float64),
    `key_s1` Nullable(String),
    `key_s2` Nullable(String),
    `key_s3` Nullable(String),
    `p_date` Date,
    INDEX key_i_idx key_i TYPE minmax GRANULARITY 1,
    INDEX key_f_idx key_f TYPE set(100) GRANULARITY 1,
    INDEX key_s1_idx key_s1 TYPE tokenbf_v1(4096, 3, 0) GRANULARITY 1,
    INDEX key_s2_idx key_s2 TYPE ngrambf_v1(3, 4096, 3, 0) GRANULARITY 1,
    INDEX key_s3_idx key_s3 TYPE bloom_filter(0.1) GRANULARITY 1
)
ENGINE = CnchMergeTree()
PARTITION BY p_date
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO test_nullable_skipindex SELECT
    number,
    number,
    number * 0.1,
    toString(number),
    toString(number),
    toString(number),
    '2022-02-02'
FROM system.numbers
LIMIT 1000000;

select sum(id) from test_nullable_skipindex where key_i = 1;
select sum(id) from test_nullable_skipindex where key_f = 1;
select sum(id) from test_nullable_skipindex where key_s1 = '1';
select sum(id) from test_nullable_skipindex where key_s2 = '1';
select sum(id) from test_nullable_skipindex where key_s3 = '1';

INSERT INTO test_nullable_skipindex SELECT
    1,
    Null,
    Null,
    Null,
    Null,
    Null,
    '2022-02-02'
FROM system.numbers
LIMIT 1000000;

system start merges test_nullable_skipindex;
system stop merges test_nullable_skipindex;
set mutations_sync = 1;
optimize table test_nullable_skipindex;

select sum(id) from test_nullable_skipindex where key_i = 1;
select sum(id) from test_nullable_skipindex where key_f = 1;
select sum(id) from test_nullable_skipindex where key_s1 = '1';
select sum(id) from test_nullable_skipindex where key_s2 = '1';
select sum(id) from test_nullable_skipindex where key_s3 = '1';

select sum(id) from test_nullable_skipindex where key_i = Null;
select sum(id) from test_nullable_skipindex where key_f = Null;
select sum(id) from test_nullable_skipindex where key_s1 = Null;
select sum(id) from test_nullable_skipindex where key_s2 = Null;
select sum(id) from test_nullable_skipindex where key_s3 = Null;

select sum(id) from test_nullable_skipindex where isNull(key_i);
select sum(id) from test_nullable_skipindex where isNull(key_f);
select sum(id) from test_nullable_skipindex where isNull(key_s1);
select sum(id) from test_nullable_skipindex where isNull(key_s2);
select sum(id) from test_nullable_skipindex where isNull(key_s3);

drop table if exists test_nullable_skipindex sync;
