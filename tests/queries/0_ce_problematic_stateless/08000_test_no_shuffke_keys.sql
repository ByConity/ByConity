
drop table if exists test.test_no_shuffle_keys_local;
drop table if exists test.test_no_shuffle_keys;

CREATE TABLE test.test_no_shuffle_keys_local
(
    `p_date` Date,
    `id` Int32,
    `event` String
)
ENGINE = MergeTree
PARTITION BY p_date
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE test.test_no_shuffle_keys
(
    `p_date` Date,
    `id` Int32,
    `event` String
)
ENGINE = Distributed(test_shard_localhost, test, test_no_shuffle_keys_local);

insert into test.test_no_shuffle_keys_local select '2022-01-01', number, 'a' from numbers(5);

set enable_distributed_stages = 1;

select count() from test.test_no_shuffle_keys as a, test.test_no_shuffle_keys as b where a.id = b.id;
select count() from test.test_no_shuffle_keys as a, test.test_no_shuffle_keys as b where a.id = b.id group by p_date;

select count() from test.test_no_shuffle_keys as a join test.test_no_shuffle_keys as b on a.id = b.id;
select count() from test.test_no_shuffle_keys as a join test.test_no_shuffle_keys as b on a.id = b.id group by p_date;

select count() from (
select count() as c from test.test_no_shuffle_keys as a join test.test_no_shuffle_keys as b on a.id = b.id group by p_date
) as a join 
(
    select count() as c from test.test_no_shuffle_keys as a join test.test_no_shuffle_keys as b on a.id = b.id group by p_date
) as b on a.c = b.c;

select count() from (
select count() as c from test.test_no_shuffle_keys as a join test.test_no_shuffle_keys as b on a.id = b.id
) as a join 
(
    select count() as c from test.test_no_shuffle_keys as a join test.test_no_shuffle_keys as b on a.id = b.id
) as b on a.c = b.c;

drop table if exists test.test_no_shuffle_keys_local;
drop table if exists test.test_no_shuffle_keys;