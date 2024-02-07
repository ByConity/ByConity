drop DATABASE if exists test_48037;
CREATE DATABASE test_48037;

use test_48037;
drop table if exists test1;
drop table if exists test2;

create table test1(a String, b String NOT NULL) engine = CnchMergeTree() order by a;
create table test2(a String, b UInt64 NOT NULL) engine = CnchMergeTree() order by a;

insert into test1 values ('a', '1');
insert into test1 values ('a', '2');
insert into test1 values ('a', '3');
insert into test1 values ('a', '4');
insert into test1 values ('a', '5');

insert into test2 values ('a', 1);
insert into test2 values ('a', 2);
insert into test2 values ('a', 3);
insert into test2 values ('a', 4);
insert into test2 values ('a', 5);

SELECT count(b) AS b2
FROM
(
    SELECT
        a,
        b
    FROM test1
    UNION ALL
    SELECT
        a,
        cast(b, 'String')
    FROM test2
);


