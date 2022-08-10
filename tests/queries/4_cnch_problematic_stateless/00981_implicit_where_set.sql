USE test;
drop table if exists test.test_eliminate;

create table test.test_eliminate (date Date, id Int32, event String) engine = CnchMergeTree partition by toYYYYMM(date) order by id;

insert into test.test_eliminate values ('2020-01-01', 1, 'a');
insert into test.test_eliminate values ('2020-01-01', 1, 'b');
insert into test.test_eliminate values ('2020-01-02', 2, 'c');

select count() from test.test_eliminate where date in '2020-01-01' settings deduce_part_eliminate = 0;
select count() from test.test_eliminate where date in '2020-01-01' settings deduce_part_eliminate = 1;
select id, count() from test.test_eliminate where date in '2020-01-01' group by id order by id settings deduce_part_eliminate = 0;
select id, count() from test.test_eliminate where date in '2020-01-01' group by id order by id settings deduce_part_eliminate = 1;
select id from test.test_eliminate where date in '2020-01-01' order by id settings deduce_part_eliminate = 0;
select id from test.test_eliminate where date in '2020-01-01' order by id settings deduce_part_eliminate = 1;

drop table if exists test.test_eliminate;
