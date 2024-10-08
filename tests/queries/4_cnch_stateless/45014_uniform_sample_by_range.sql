drop table if exists uniform_sample;
CREATE TABLE uniform_sample(`x` UInt8) Engine = CnchMergeTree() order by x;


insert into uniform_sample select intDiv(number, 8192 * 64) from system.numbers limit 8192 * 64 * 64;

set enable_deterministic_sample_by_range=1;
set uniform_sample_by_range=1;
select if(res >= 55, 'OK', concat('FAIL: ', toString(res))) from (
    select uniq(x) as res from uniform_sample sample 1/16
);
-- set uniform_sample_by_range=0;
-- select if(res < 55, 'OK', concat('FAIL: ', toString(res))) from (
--     select uniq(x) as res from uniform_sample sample 1/16
-- );

drop table if exists uniform_sample;
