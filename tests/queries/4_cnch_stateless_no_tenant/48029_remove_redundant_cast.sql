use test;

set enable_optimizer=1;
set enable_remove_redundant_cast=1;

drop table if exists table_48029;
CREATE TABLE table_48029 (
    `id` LowCardinality(UInt32),
    `server_time` UInt64,
    `string_params` Map(String, LowCardinality(Nullable(String))) CODEC(ZSTD(1))
) ENGINE = CnchMergeTree PARTITION BY (id) CLUSTER BY id INTO 1000 BUCKETS
ORDER BY id;

explain select * from table_48029 where CAST(string_params{'name_list'}, 'Nullable(String)')  = 'a';
explain select * from table_48029 where CAST(string_params{'name_list'}, 'LowCardinality(Nullable(String))')  = 'a';
explain select * from table_48029 where CAST(server_time, 'UInt64') = 1;

-- redundant cast in projection is not supported
explain select CAST(string_params{'name_list'}, 'Nullable(String)') from table_48029;
explain select CAST(string_params{'name_list'}, 'LowCardinality(Nullable(String))') from table_48029;
explain select CAST(server_time, 'UInt64') from table_48029;
