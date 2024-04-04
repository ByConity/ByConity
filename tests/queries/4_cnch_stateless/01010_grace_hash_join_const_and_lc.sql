select s1.x, s2.x from (select 1 as x) s1 left join (select 1 as x) s2 on s1.x=s2.x settings join_algorithm = 'grace_hash', enum_replicate=0;
select * from (select materialize(2) as x) s1 left join (select 2 as x) s2 on s1.x=s2.x settings join_algorithm = 'grace_hash', enum_replicate=0;
select * from (select 3 as x) s1 left join (select materialize(3) as x) s2 on s1.x=s2.x settings join_algorithm = 'grace_hash', enum_replicate=0;
select * from (select toLowCardinality(4) as x) s1 left join (select 4 as x) s2 on s1.x=s2.x settings join_algorithm = 'grace_hash', enum_replicate=0;
select * from (select 5 as x) s1 left join (select toLowCardinality(5) as x) s2 on s1.x =s2.x settings join_algorithm = 'grace_hash', enum_replicate=0;
select s1.x, s2.x from (select number as x from system.numbers LIMIT 10) s1 left join (select number as x from system.numbers LIMIT 10) s2 on s1.x=s2.x order by s1.x settings join_algorithm = 'grace_hash', max_rows_in_join=1, enum_replicate=0