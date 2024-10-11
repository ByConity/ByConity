drop table if exists ext_s3_table_1;
create table ext_s3_table_1
(
    `k` Int32,
    `m` Int32
)
engine = CnchS3("http://minio:9000/cnch/test_s3_ext_table/test_1.csv", 'CSV', 'none', 'minio', 'minio123');

set overwrite_current_file = 1;
insert into ext_s3_table_1 values (1, 1);
select * from ext_s3_table_1; -- 1 1

drop table if exists ext_s3_table_2;
create table ext_s3_table_2
(
    `k` Int32,
    `m` Int32
)
engine = CnchS3("http://minio:9000/cnch/test_s3_ext_table/partition_*_test_2.csv", 'CSV', 'none', 'minio', 'minio123');

insert into ext_s3_table_2 values (2, 1)(2, 2);
select * from ext_s3_table_2; -- 2 1; 2 2
select count() from (select count(*) from ext_s3_table_2 group by _path); -- 1

set insert_new_file = 0;
set overwrite_current_file = 0;
insert into ext_s3_table_2 values (2, 1)(2, 2); --{serverError 36}

drop table if exists ext_s3_table_3;
create table ext_s3_table_3
(
    `k` Int32,
    `m` Int32
)
engine = CnchS3("http://minio:9000/cnch/test_s3_ext_table/partition_*_test_3.csv", 'CSV', 'none', 'minio', 'minio123') partition by k;

set overwrite_current_file = 1;
insert into ext_s3_table_3 values (31, 1)(32, 2);
select * from ext_s3_table_3 order by k; -- 31 1; 32 2
select count(*) from ext_s3_table_2 group by _path; -- 2

drop table if exists s3_ext_table_source;
create table s3_ext_table_source
(
    `k` Int32,
    `m` Int32
)
engine = CnchMergeTree
partition by k
order by m;

drop table if exists ext_s3_table_4;
create table ext_s3_table_4
(
    `k` Int32,
    `m` Int32
)
engine =CnchS3("http://minio:9000/cnch/test_s3_ext_table/partition_*_test_4.csv", 'CSV', 'none', 'minio', 'minio123');

insert into s3_ext_table_source values (4, 1)(4, 2);
select * from s3_ext_table_source; -- 4 1; 4 2

insert into ext_s3_table_4 values (4, 3)(4, 4);
select * from ext_s3_table_4; -- 4 3; 4 4
insert overwrite ext_s3_table_4 select * from s3_ext_table_source;
select * from ext_s3_table_4; -- 4 1; 4 2
