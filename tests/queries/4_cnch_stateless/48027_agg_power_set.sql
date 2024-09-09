create database if not exists zx_test;
use zx_test;

drop table if exists customer;

create table customer
(
    item_id Int64,
    name String
) 
ENGINE = CnchMergeTree()
order by item_id;


set enable_optimizer=1;
set enable_sharding_optimize=1;

explain
select count()
from (
select name, item_id
    from customer
    group by item_id, name
) group by item_id;

explain
select count()
from 
(
    select item_id, max(name) over (partition by item_id order by name) as res
    FROM 
    (
        select item_id, name
        from customer
        group by item_id, name
    )
    where item_id between 100 and 200
);
