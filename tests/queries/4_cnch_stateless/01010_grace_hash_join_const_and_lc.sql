select s1.x, s2.x from (select 1 as x) s1 left join (select 1 as x) s2 on s1.x=s2.x settings enable_optimizer=0, join_algorithm = 'grace_hash';
select * from (select materialize(2) as x) s1 left join (select 2 as x) s2 on s1.x=s2.x settings enable_optimizer=0, join_algorithm = 'grace_hash';
select * from (select 3 as x) s1 left join (select materialize(3) as x) s2 on s1.x=s2.x settings enable_optimizer=0, join_algorithm = 'grace_hash';
select * from (select toLowCardinality(4) as x) s1 left join (select 4 as x) s2 on s1.x=s2.x settings enable_optimizer=0, join_algorithm = 'grace_hash';
select * from (select 5 as x) s1 left join (select toLowCardinality(5) as x) s2 on s1.x =s2.x settings enable_optimizer=0, join_algorithm = 'grace_hash';