
drop table if exists test_distribution;

create table test_distribution (`int32_data` Int32, `uint64_data` UInt64, `float_data` Float64) engine=CnchMergeTree order by(int32_data, uint64_data);

insert into test_distribution values(-5, 1, 1.1);
insert into test_distribution values(-4, 2, 2.2);
insert into test_distribution values(-3, 3, 3.3);
insert into test_distribution values(-2, 4, 4.4);
insert into test_distribution values(-1, 5, 5.5);
insert into test_distribution values(0, 6, 6.6);
insert into test_distribution values(1, 7, 7.7);
insert into test_distribution values(2, 8, 8.8);
insert into test_distribution values(3, 9, 9.9);
insert into test_distribution values(4, 10, 10.1);

select distribution(-5, 0, 2)(int32_data) from (select int32_data from test_distribution);
select distribution(-5, 3, 3)(int32_data) from (select int32_data from test_distribution);
select distribution(-1, 1, 5)(int32_data) from (select int32_data from test_distribution);
select distribution(1, 1, 5)(int32_data) from (select int32_data from test_distribution);
select distribution(1.0, 1, 5)(int32_data) from (select int32_data from test_distribution);

select distribution(1, 0, 2)(uint64_data) from (select uint64_data from test_distribution);
select distribution(1, 5, 2)(uint64_data) from (select uint64_data from test_distribution);
select distribution(1, 2, 2)(uint64_data) from (select uint64_data from test_distribution);
select distribution(-1, 2, 2)(uint64_data) from (select uint64_data from test_distribution);
select distribution(1.0, 2, 2)(uint64_data) from (select uint64_data from test_distribution);

select distribution(1.1, 0, 2)(float_data) from (select float_data from test_distribution);
select distribution(1.1, 2.2, 5)(float_data) from (select float_data from test_distribution);
select distribution(1.0, 2.0, 5)(float_data) from (select float_data from test_distribution);
select distribution(1, 2, 5)(float_data) from (select float_data from test_distribution);
select distribution(1, 2.0, 5)(float_data) from (select float_data from test_distribution);
select distribution(1.1, 2.2, 5)(toFloat32(float_data)) from (select float_data from test_distribution);

drop table if exists test_distribution;