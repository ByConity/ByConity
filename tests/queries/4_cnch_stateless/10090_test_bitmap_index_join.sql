drop table if exists test_index_join_123_local;
drop table if exists test_index_join_124_local;

create table test_index_join_123_local (p_date Date, id Int32) engine = CnchMergeTree partition by p_date order by id;
create table test_index_join_124_local (p_date Date, id Int32, vids Array(Int32) BitmapIndex) engine = CnchMergeTree partition by p_date order by id;

insert into test_index_join_123_local select '2023-01-01', number from numbers(5);
insert into test_index_join_124_local select '2023-01-01', number, [number] from numbers(5);

select id, b.vids from test_index_join_123_local as a join test_index_join_124_local as b on a.id = b.id where id = 1 and arraySetCheck(vids, 1);
select id, b.vids from test_index_join_123_local as a join test_index_join_124_local as b on a.id = b.id where id = 1 and arraySetCheck(vids, 1) settings optimize_move_to_prewhere = 0;
select id, b.vids from test_index_join_123_local as a join test_index_join_124_local as b on a.id = b.id where id = 1 and arraySetCheck(vids, 1) settings enable_ab_index_optimization = 1;

drop table test_index_join_123_local;
drop table test_index_join_124_local;
