drop table if exists distributed_max_parallel_size_overflow;

CREATE TABLE distributed_max_parallel_size_overflow
(
    `number` UInt64,
    `num2` UInt64
)
ENGINE = CnchMergeTree
ORDER BY number;

insert into distributed_max_parallel_size_overflow values (0, 1);
insert into distributed_max_parallel_size_overflow values (3, 4);

set enable_optimizer = 1, enable_optimizer_fallback = 0, bsp_mode = 1;
select * from distributed_max_parallel_size_overflow order by number settings distributed_max_parallel_size = 1;
select * from distributed_max_parallel_size_overflow order by number settings distributed_max_parallel_size = 2;
select * from distributed_max_parallel_size_overflow order by number settings distributed_max_parallel_size = 3;
select * from distributed_max_parallel_size_overflow order by number settings distributed_max_parallel_size = 4;
select * from distributed_max_parallel_size_overflow order by number settings distributed_max_parallel_size = 12;

-- In mpp mode, distributed_max_parallel_size will be aligned to the number of workers.
set bsp_mode=0;
select * from distributed_max_parallel_size_overflow order by number settings distributed_max_parallel_size = 12;

drop table distributed_max_parallel_size_overflow;
