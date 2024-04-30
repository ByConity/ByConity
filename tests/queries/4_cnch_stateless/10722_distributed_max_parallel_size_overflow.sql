use test;

CREATE TABLE overflow
(
    `number` UInt64,
    `num2` UInt64
)
ENGINE = CnchMergeTree
ORDER BY number;

insert into overflow values (0, 1);
insert into overflow values (3, 4);

set enable_optimizer = 1, enable_optimizer_fallback = 0, bsp_mode = 1;
select * from overflow order by number settings distributed_max_parallel_size = 1;
select * from overflow order by number settings distributed_max_parallel_size = 2;
select * from overflow order by number settings distributed_max_parallel_size = 3;
select * from overflow order by number settings distributed_max_parallel_size = 4;

drop table overflow;
