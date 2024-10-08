CREATE TABLE 00396_test_uuid
(
    `id` Nullable(Int32),
    `id2` Nullable(UUID) DEFAULT generateUUIDv4()
)
ENGINE = CnchMergeTree
ORDER BY tuple()
UNIQUE KEY tuple()
SETTINGS partition_level_unique_keys = 0, storage_policy = 'cnch_default_hdfs', allow_nullable_key = 1, storage_dialect_type = 'MYSQL', index_granularity = 8192;
insert into 00396_test_uuid(id) values(1)(2);
select id, bitShiftLeft(toUInt128(high),64) + toUInt128(low) = x from (select id, UUIDToUInt64High(id2) as high, UUIDToUInt64Low(id2) as low, toUInt128(id2) as x from 00396_test_uuid);
drop table 00396_test_uuid;
