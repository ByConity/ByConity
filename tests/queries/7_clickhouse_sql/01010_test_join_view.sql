drop table if exists view_test_join_view;
drop table if exists test_join_view;

create table test_join_view (p_date Date, id Int32, event String) engine = CnchMergeTree partition by p_date order by id;
create VIEW view_test_join_view (p_date Date, id Int32) AS select p_date, id from test_join_view as a join test_join_view as b on a.id = b.id;

insert into test_join_view values ('2021-01-01', 1, 'a');

select * from view_test_join_view as a join view_test_join_view as b on a.id = b.id;

drop table if exists view_test_join_view;
drop table if exists test_join_view;