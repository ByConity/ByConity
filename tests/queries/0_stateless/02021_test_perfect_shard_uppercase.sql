drop table if exists test.test_perfect_shard_uppercase_local;
drop table if exists test.test_perfect_shard_uppercase;

CREATE TABLE test.test_perfect_shard_uppercase_local
(
    `id` Int32
)
ENGINE = MergeTree
PARTITION BY id
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE test.test_perfect_shard_uppercase
(
    `id` Int32
)
ENGINE = Distributed('test_shard_localhost', 'test', 'test_perfect_shard_uppercase_local', rand());

insert into test.test_perfect_shard_uppercase_local select number from numbers(10);

select SUM(id) from test.test_perfect_shard_uppercase settings distributed_perfect_shard = 1, prefer_localhost_replica = 0;
select sum(id) from test.test_perfect_shard_uppercase settings distributed_perfect_shard = 1, prefer_localhost_replica = 0;

drop table if exists test.test_perfect_shard_uppercase_local;
drop table if exists test.test_perfect_shard_uppercase;
