set dialect_type='MYSQL';

drop table if exists tbx_1;
create table tbx_1 (p int NOT NULL, x tinyint, y String, z float) engine=CnchMergeTree() order by p;

select 'add column';
alter table tbx_1 add column z1 float;
alter table tbx_1 add z2 String NOT NULL;
alter table tbx_1 add column z3 Array(int);
alter table tbx_1 add column z4 DateTime NULL default '2024-02-24 17:00:00';
describe table tbx_1;

insert into table tbx_1(p, x, y, z, z1, z2, z3) select number, number, 'hello', 1.0, NULL, 'world', [1,2,3] from system.numbers limit 100;
select * from tbx_1 limit 3;  --skip_if_readonly_ci

select 'modify column';
alter table tbx_1 modify column x int;
alter table tbx_1 modify column y String default 'hello';
alter table tbx_1 modify column z double default 1.2;
alter table tbx_1 modify z2 String NULL;
describe table tbx_1;
select * from tbx_1 limit 3;  --skip_if_readonly_ci

alter table tbx_1 modify column z float; -- { serverError 70 }
alter table tbx_1 modify column x decimal(10,3); -- { serverError 70 }
alter table tbx_1 modify column x String; -- { serverError 70 }
alter table tbx_1 modify column x DateTime; -- { serverError 70 }

alter table tbx_1 modify column z4 Array(float); -- { serverError 70 }
alter table tbx_1 modify column z4 Array(String); -- { serverError 70 }
alter table tbx_1 modify column x float NOT NULL; -- { serverError 70 }

select 'alter index';
alter table tbx_1 add index idx(y); -- { serverError 524 }
alter table tbx_1 add key idx(y); -- { serverError 524 }
alter table tbx_1 drop index idx; -- { serverError 524 }
alter table tbx_1 add clustered key cidx(y); -- {serverError 524 }
-- alter table tbx_1 drop clustered index cidx; -- {serverError 524 }

-- if alter previous columns, there could be exception due to the previous alter ops not finished yet.
-- therefore, create new table with new columns
drop table if exists tbx_1;
create table tbx_2 (p int NOT NULL, x tinyint, y String, z float) engine=CnchMergeTree() order by p;

insert into table tbx_2 select number, number, 'hello', 1.0 from system.numbers limit 100;
select * from tbx_2 limit 3;

select 'rename column';
alter table tbx_2 rename column x to x1;
describe table tbx_2;
select * from tbx_2 limit 3;

select 'drop column';
alter table tbx_2 drop column y;
alter table tbx_2 drop z;
describe table tbx_2;
select * from tbx_2 limit 3;

select 'primary key';
alter table tbx_2 add column z5 int primary key; -- { serverError 524 }
alter table tbx_2 modify column p bigint; -- { serverError 524 }
alter table tbx_2 drop p; -- { serverError 524 }

drop table if exists tbx_2;

use test;
drop table if exists tbx_alter_test_old_60010;
drop table if exists tbx_alter_test_rename_60010;
create table tbx_alter_test_old_60010 (p int NOT NULL, x tinyint, y String, z float) engine=CnchMergeTree() order by p;
select 'rename table';
alter table tbx_alter_test_old_60010 rename tbx_alter_test_rename_60010;
alter table tbx_alter_test_rename_60010 rename tbx_alter_test_old_60010;
alter table test.tbx_alter_test_old_60010 rename test.tbx_alter_test_rename_60010;
alter table test.tbx_alter_test_rename_60010 rename test.tbx_alter_test_old_60010;
use default;
alter table test.tbx_alter_test_old_60010 rename test.tbx_alter_test_rename_60010;
alter table test.tbx_alter_test_rename_60010 rename tbx_alter_test_old_60010; -- { serverError 48 }
alter table test.tbx_alter_test_rename_60010 rename test.tbx_alter_test_old_60010;

drop table if exists test.tbx_alter_test_old_60010;
