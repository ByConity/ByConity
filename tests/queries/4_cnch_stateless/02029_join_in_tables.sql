drop table if exists test_join_in_table_123;
create table test_join_in_table_123 (p_date Date, id Int32) engine = CnchMergeTree partition by p_date order by id;
select count() from 
    (select * from test_join_in_table_123) as a 
    join (select * from test_join_in_table_123) as b on a.id = b.id 
    join (select * from test_join_in_table_123) as c on a.id = c.id 
    where a.id in (select id from test_join_in_table_123);
drop table if exists test_join_in_table_123;
