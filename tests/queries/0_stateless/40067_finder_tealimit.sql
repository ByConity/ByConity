drop table if exists finder_tealimit_test;
drop table if exists finder_tealimit_test_dist;

create table finder_tealimit_test (a Int32, b Int32, c Int32) engine = MergeTree order by a;

insert into finder_tealimit_test values (1,1,1),(1,1,1)(1,2,1)(1,3,1),(1,4,1);
insert into finder_tealimit_test values (2,1,1),(2,1,1)(2,2,1)(2,4,1),(2,4,1);

create table finder_tealimit_test_dist as finder_tealimit_test engine = Distributed(test_shard_localhost, currentDatabase(), finder_tealimit_test);

select a as t1, b as t2, count() as cnt
    from finder_tealimit_test_dist
    group by t1, t2
    order by t1 desc, t2 desc
    tealimit 10 group t1 order cnt desc settings tealimit_order_keep = 1;

drop table if exists finder_tealimit_test;
drop table if exists finder_tealimit_test_dist;