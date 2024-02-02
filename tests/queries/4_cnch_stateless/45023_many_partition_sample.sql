create database if not exists test;
drop table if exists part_sample;

CREATE TABLE part_sample(`x` UInt64, `part` UInt64) ENGINE = CnchMergeTree() partition by part order by x;

set max_partitions_per_insert_block=0;
insert into part_sample(x, part) select number, intDiv(number, 4 * 8192) from system.numbers limit 8192 * 64 * 4;

set enable_deterministic_sample_by_range=1;
set uniform_sample_by_range=1;
set ensure_one_mark_in_part_when_sample_by_range=1;
select if(res >= 8192 * 32, 'OK', concat('FAIL: ', toString(res))) from (
    select count(x) as res from part_sample sample 1/128
);

set ensure_one_mark_in_part_when_sample_by_range=0;
select if(res <= 8192 * 10, 'OK', concat('FAIL: ', toString(res))) from (
    select count(x) as res from part_sample sample 1/128
);

drop table if exists part_sample;
