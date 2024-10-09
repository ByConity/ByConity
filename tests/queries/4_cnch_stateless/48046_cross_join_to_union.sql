drop table if exists test_48046;
drop table if exists test_48046_local;

drop table if exists test_48046_1;
drop table if exists test_48046_1_local;

set dialect_type = 'CLICKHOUSE';
set enable_optimizer = 1;

create table test_48046 (a String, b Nullable(String), c Int64, d Int64) engine = CnchMergeTree() order by a;

create table test_48046_1 (a String, b Nullable(String), c Int64, d Int64) engine = CnchMergeTree() order by a ;

insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('2', '2', 2, 2);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('1', '1', 1, 1);
insert into test_48046 values('3', '3', 3, 3);
insert into test_48046 values('4', '4', 4, 4);
insert into test_48046 values('5', '5', 5, 5);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('6', '6', 6, 6);
insert into test_48046 values('7', '7', 7, 7);
insert into test_48046 values('8', '8', 8, 8);
insert into test_48046 values('9', '9', 9, 9);
insert into test_48046 values('10', '10', 10, 10);
insert into test_48046 values('11', '11', 11, 11);

insert into test_48046_1 values('1', '1', 11, 11);
insert into test_48046_1 values('1', '1', 11, 11);
insert into test_48046_1 values('1', '1', 11, 11);
insert into test_48046_1 values('1', '1', 11, 11);
insert into test_48046_1 values('1', '1', 11, 11);
insert into test_48046_1 values('1', '1', 11, 11);
insert into test_48046_1 values('2', '2', 12, 12);
insert into test_48046_1 values('2', '2', 12, 12);
insert into test_48046_1 values('2', '2', 12, 12);
insert into test_48046_1 values('2', '2', 12, 12);
insert into test_48046_1 values('2', '2', 12, 12);
insert into test_48046_1 values('2', '2', 12, 12);
insert into test_48046_1 values('2', '2', 12, 12);
insert into test_48046_1 values('2', '2', 12, 12);
insert into test_48046_1 values('3', '3', 13, 13);
insert into test_48046_1 values('3', '3', 13, 13);
insert into test_48046_1 values('3', '3', 13, 13);
insert into test_48046_1 values('3', '3', 13, 13);
insert into test_48046_1 values('3', '3', 13, 13);
insert into test_48046_1 values('3', '3', 13, 13);
insert into test_48046_1 values('3', '3', 13, 13);
insert into test_48046_1 values('3', '3', 13, 13);
insert into test_48046_1 values('3', '3', 13, 13);
insert into test_48046_1 values('3', '3', 13, 13);
insert into test_48046_1 values('4', '4', 14, 14);
insert into test_48046_1 values('4', '4', 14, 14);
insert into test_48046_1 values('4', '4', 14, 14);
insert into test_48046_1 values('4', '4', 14, 14);
insert into test_48046_1 values('4', '4', 14, 14);
insert into test_48046_1 values('4', '4', 14, 14);
insert into test_48046_1 values('4', '4', 14, 14);
insert into test_48046_1 values('4', '4', 14, 14);
insert into test_48046_1 values('4', '4', 14, 14);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('5', '5', 15, 15);
insert into test_48046_1 values('6', '6', 16, 16);
insert into test_48046_1 values('7', '7', 17, 17);
insert into test_48046_1 values('8', '8', 18, 18);
insert into test_48046_1 values('9', '9', 19, 19);
insert into test_48046_1 values('10', '10', 110, 110);
insert into test_48046_1 values('11', '11', 111, 111);

-- match (A = B) OR (C = D)
set enable_cross_join_to_union=1;
select count(*) from test_48046, test_48046_1 where ((test_48046.a = test_48046_1.a) or (test_48046.b = test_48046_1.b)) ;

set enable_cross_join_to_union=0;
select count(*) from test_48046, test_48046_1 where ((test_48046.a = test_48046_1.a) or (test_48046.b = test_48046_1.b)) ;

-- match (A = B) OR (C = D) AND (E != F)
set enable_cross_join_to_union=1;
select count(*) from test_48046, test_48046_1 where ((test_48046.a = test_48046_1.a) or (test_48046.b = test_48046_1.b)) and (test_48046.c != test_48046_1.c);

set enable_cross_join_to_union=0;
select count(*) from test_48046, test_48046_1 where ((test_48046.a = test_48046_1.a) or (test_48046.b = test_48046_1.b)) and (test_48046.c != test_48046_1.c);

-- match (E != F) AND (A = B) OR (C = D) 
set enable_cross_join_to_union=1;
select count(*) from test_48046, test_48046_1 where (test_48046.c != test_48046_1.c) and ((test_48046.a = test_48046_1.a) or (test_48046.b = test_48046_1.b)) ;

set enable_cross_join_to_union=0;
select count(*) from test_48046, test_48046_1 where (test_48046.c != test_48046_1.c) and ((test_48046.a = test_48046_1.a) or (test_48046.b = test_48046_1.b)) ;

-- NOT match (E != F) AND (A = B) OR (C = D) AND (H != G)
set enable_cross_join_to_union=1;
select count(*) from test_48046, test_48046_1 where (test_48046.a != test_48046_1.a) and ((test_48046.c = test_48046_1.c) or (test_48046.d = test_48046_1.d)) and (test_48046.b != test_48046_1.b);

set enable_cross_join_to_union=0;
select count(*) from test_48046, test_48046_1 where (test_48046.a != test_48046_1.a) and ((test_48046.c = test_48046_1.c) or (test_48046.d = test_48046_1.d)) and (test_48046.b != test_48046_1.b);

