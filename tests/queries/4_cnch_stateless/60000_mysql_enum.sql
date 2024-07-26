set dialect_type='MYSQL';
drop table if exists mysql_enum;
create table mysql_enum(
    id Int32,
    name enum('test' = 1, 'tmp' = 2)
);
insert into mysql_enum values(1, 1), (1, 2), (2, 1), (2, 2);
select * from mysql_enum where name = 'test';
drop table mysql_enum;
