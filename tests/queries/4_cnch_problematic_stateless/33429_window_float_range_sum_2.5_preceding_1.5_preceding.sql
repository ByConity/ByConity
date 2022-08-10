DROP DATABASE IF EXISTS test;
CREATE DATABASE test;

DROP TABLE IF EXISTS test.float_range_sum_2_5_preceding_1_5_preceding;

CREATE TABLE test.float_range_sum_2_5_preceding_1_5_preceding(`a` Float32, `b` Float32) 
    ENGINE = CnchMergeTree() 
    PARTITION BY `a` 
    PRIMARY KEY `a` 
    ORDER BY `a` 
    SETTINGS index_granularity = 8192;
    
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(0,0.1);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(0,0.5);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(0,1.2);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(0,5.3);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(0,8.5);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(0,14.6);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(0,14.8);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(0,14.9);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(0,22.2);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(1,3.6);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(1,3.5);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(1,4.6);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(1,2.2);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(1,6.5);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(1,7.2);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(1,9.1);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(1,3.2);
insert into test.float_range_sum_2_5_preceding_1_5_preceding values(1,6.5);


select a, sum(b) over (partition by a order by toFloat64(b) RANGE BETWEEN 2.5 PRECEDING AND 1.5 Preceding) as res
FROM test.float_range_sum_2_5_preceding_1_5_preceding
order by a, res;

DROP TABLE test.float_range_sum_2_5_preceding_1_5_preceding;
