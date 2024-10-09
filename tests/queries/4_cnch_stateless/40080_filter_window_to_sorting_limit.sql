SET enable_optimizer = 1, enable_filter_window_to_sorting_limit = 1;

DROP TABLE IF EXISTS t40080_tfpoc;

CREATE TABLE t40080_tfpoc
(
    uuid String,
    package_name String,
    recv_timestamp UInt64
) ENGINE = CnchMergeTree ORDER BY uuid;

explain stats=0
select
    package_name
from (
    select
        package_name,
        row_number() over(order by recv_timestamp desc ) as rn
    from t40080_tfpoc
    where uuid = 'xx' and recv_timestamp > 100000000
)
where rn <= 10;

explain stats=0
select
    package_name
from (
    select
        package_name,
        uuid,
        row_number() over(order by recv_timestamp desc ) as rn
    from t40080_tfpoc
    where recv_timestamp > 100000000
)
where rn <= 10 and uuid = 'xx';

explain stats=0
select 
    t1.package_name
from
(
    select
        package_name,
        rn,
        uuid
    from (
        select
            package_name,
            uuid,
            row_number() over(order by recv_timestamp desc ) as rn
        from t40080_tfpoc
        where recv_timestamp > 100000000
    )
) t1
    join
t40080_tfpoc t2
on t1.package_name = t2.package_name
where t1.rn <= 10 and t1.uuid = 'xx';


DROP TABLE IF EXISTS t40080_tfpoc;
