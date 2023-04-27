USE test;
drop table if exists test.test_subquery_in_prewhere;

create table test.test_subquery_in_prewhere (date Date, id Int32, vids Array(Int32) BLOOM) engine = CnchMergeTree partition by date order by id;

insert into test.test_subquery_in_prewhere values ('2019-01-01', 1, [1]);

select id from test.test_subquery_in_prewhere prewhere id in ( select id from test.test_subquery_in_prewhere where arraySetCheck(vids, 1));

drop table if exists test.test_subquery_in_prewhere;
