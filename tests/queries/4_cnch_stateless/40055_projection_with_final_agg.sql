SET max_threads=1;
SET exchange_source_pipeline_threads=1;

DROP TABLE IF EXISTS t40055;

CREATE TABLE t40055
(
    `company_id` Int32,
    `id` UInt32,
    `user_id` Nullable(UInt32),
    `user_name` LowCardinality(String),
    `operator_time` DateTime,
    `operator_type` LowCardinality(String),
    `operator_device_type` LowCardinality(String),
    `operator_service_name` LowCardinality(String),
    `operator_module_name` LowCardinality(String),
    `unit_id` Nullable(Int8),
    `unit_name` Nullable(String),
    PROJECTION p_01
    (
        SELECT
            toDate(operator_time),
            user_id,
            company_id,
            count(1)
        GROUP BY
            toDate(operator_time),
            user_id,
            company_id
    )
)
ENGINE = CnchMergeTree()
PARTITION BY toDate(operator_time)
ORDER BY operator_time
SETTINGS index_granularity = 8192;


insert into  t40055 select
 number % 7 + 1000 as company_id,
 0 as id,
 number % 100 as user_id,
 '' as user_name,
'2020-01-04 08:00:00'  operator_time,
 '' as operator_type,
 '' as operator_device_type,
 '' as operator_service_name,
 '' as operator_module_name,
 0 as unit_id,
 '' as unit_name  from system.numbers limit 100000 settings enable_optimizer=0;

-- explain pipeline
-- select d,company_id, count(1) as c1
-- from
-- (
--     select toDate(operator_time) as d,user_id ,company_id,count(1) as c
--     from t40055 bl
--     where toDate(operator_time) >= '2020-01-01'
--     group by toDate(operator_time),user_id,company_id order by c desc
-- )
-- group by d,company_id
-- order by d desc, company_id, c1 desc limit 10
-- settings max_threads=30,enable_optimizer=1,optimizer_projection_support=1, enable_sharding_optimize = 1;

select d,company_id, count(1) as c1
from
(
    select toDate(operator_time) as d,user_id ,company_id,count(1) as c
    from t40055 bl
    where toDate(operator_time) >= '2020-01-01'
    group by toDate(operator_time),user_id,company_id order by c desc
)
group by d,company_id
order by d desc, company_id, c1 desc limit 10
settings max_threads=30,enable_optimizer=1,optimizer_projection_support=1, enable_sharding_optimize = 1;

DROP TABLE IF EXISTS t40055;
