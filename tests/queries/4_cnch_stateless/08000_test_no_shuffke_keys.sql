
drop table if exists test_no_shuffle_keys_local;

CREATE TABLE test_no_shuffle_keys_local
(
    `p_date` Date,
    `id` Int32,
    `event` String
)
ENGINE = CnchMergeTree
PARTITION BY p_date
ORDER BY id
SETTINGS index_granularity = 8192;

insert into test_no_shuffle_keys_local select '2022-01-01', number, 'a' from numbers(5);

set enable_distributed_stages = 1;

select count() from test_no_shuffle_keys_local as a, test_no_shuffle_keys_local as b where a.id = b.id;
select count() from test_no_shuffle_keys_local as a, test_no_shuffle_keys_local as b where a.id = b.id group by p_date;

select count() from test_no_shuffle_keys_local as a join test_no_shuffle_keys_local as b on a.id = b.id;
select count() from test_no_shuffle_keys_local as a join test_no_shuffle_keys_local as b on a.id = b.id group by p_date;

select count() from (
select count() as c from test_no_shuffle_keys_local as a join test_no_shuffle_keys_local as b on a.id = b.id group by p_date
) as a join 
(
    select count() as c from test_no_shuffle_keys_local as a join test_no_shuffle_keys_local as b on a.id = b.id group by p_date
) as b on a.c = b.c;

select count() from (
select count() as c from test_no_shuffle_keys_local as a join test_no_shuffle_keys_local as b on a.id = b.id
) as a join 
(
    select count() as c from test_no_shuffle_keys_local as a join test_no_shuffle_keys_local as b on a.id = b.id
) as b on a.c = b.c;

drop table if exists test_no_shuffle_keys_local;