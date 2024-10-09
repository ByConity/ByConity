set dialect_type='MYSQL';

drop table if exists mysql_dt;
create table mysql_dt(
    id Int32(5, 7),
    name String
);
show create table mysql_dt;
drop table mysql_dt;
