USE test;
DROP TABLE IF EXISTS test.t;

create table test.t(d Date) engine CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY d;

insert into test.t values ('2018-02-20');

select count() from test.t where toDayOfWeek(d) in (2);

DROP TABLE test.t;
