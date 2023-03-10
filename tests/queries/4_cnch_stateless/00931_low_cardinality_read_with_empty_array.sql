
drop table if exists lc;
create table lc (key UInt64, value Array(LowCardinality(String))) engine = CnchMergeTree order by key;
insert into lc select number, if(number < 10000 or number > 100000, [toString(number)], emptyArrayString()) from system.numbers limit 200000;
select * from lc where (key < 100 or key > 50000) and not has(value, toString(key)) and length(value) == 1 limit 10 settings max_block_size = 8192, max_threads = 1;

drop table if exists lc;

