drop table if exists aliases_test;

create table aliases_test (date default today(), id default rand(), array default [0, 1, 2]) engine=MergeTree(date, id, 1);

insert into aliases_test (id) values (0);
select array from aliases_test;

alter table aliases_test modify column array alias [0, 1, 2];
select array from aliases_test;

alter table aliases_test modify column array default [0, 1, 2];
select array from aliases_test;

alter table aliases_test add column struct.key_ Array(UInt8) default [0, 1, 2], add column struct.value_ Array(UInt8) default array;
select struct.key_, struct.value_ from aliases_test;

alter table aliases_test modify column struct.value_ alias array;
select struct.key_, struct.value_ from aliases_test;

select struct.key_, struct.value_ from aliases_test array join struct;
select struct.key_, struct.value_ from aliases_test array join struct as struct;
select class.key_, class.value_ from aliases_test array join struct as class;

drop table aliases_test;
