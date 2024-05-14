DROP TABLE IF EXISTS offloading_with_query_plan;
CREATE TABLE offloading_with_query_plan (
    id Int32,
    a Int32,
    b String
) ENGINE = CnchMergeTree() ORDER BY id CLUSTER BY id INTO 2 Buckets;

insert into offloading_with_query_plan values (1,1,1), (3,3,3), (2,2,2);

set offloading_with_query_plan=1;
select id, a, b from offloading_with_query_plan where 1=0;
select id, a, b from offloading_with_query_plan order by id;
insert into offloading_with_query_plan select id, a, b from offloading_with_query_plan;
