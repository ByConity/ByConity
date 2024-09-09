drop table if exists test;
create table test (x UInt64) engine=CnchMergeTree() order by tuple();
set insert_null_as_default=1;
insert into test select number % 2 ? NULL : 42 as x from numbers(2);
select * from test order by x;
drop table test;

create table test (x LowCardinality(String) default 'Hello') engine=CnchMergeTree() order by tuple();
insert into test select (number % 2 ? NULL : 'World')::LowCardinality(Nullable(String)) from numbers(2);
select * from test order by x;
drop table test;
select NULL::LowCardinality(Nullable(String));
drop table if exists source_table;
drop table if exists target_table;

CREATE TABLE source_table
(
    c1 UInt8,
    str Nullable(String),
    lc_str LowCardinality(Nullable(String)),
    dt Nullable(DateTime),
    lc_dt LowCardinality(Nullable(DateTime)),
    d Nullable(Decimal(38,6))
)
ENGINE = CnchMergeTree
ORDER BY c1;

CREATE TABLE target_table
(
    c1 UInt8,
    str String,
    str_default String default 'Hello',
    lc_str LowCardinality(String),
    lc_str_default LowCardinality(String) default 'World',
    dt_default DateTime default '2024-01-04 23:00:00',
    lc_dt_default LowCardinality(DateTime) default '2024-01-05 00:00:00',
    d Decimal(38,6),
    d_default Decimal(38,6) default 0.01
)
ENGINE = CnchMergeTree
ORDER BY c1;

insert into source_table values(1, NULL, NULL, NULL, NULL, NULL), (2, 'a', 'b', '2000-01-01 00:00:00', '2000-12-31 23:59:59', 0.02);

insert into target_table select c1, str, str, lc_str, lc_str, dt, lc_dt, d, d from source_table;
select * from target_table order by c1;

insert into target_table select c1+10, upper(str), upper(str), upper(lc_str), upper(lc_str), dt+1, lc_dt+1, d*2, d*2 from source_table;
select * from target_table order by c1;

insert into target_table select c1+10, upper(str), upper(str), upper(lc_str), upper(lc_str), dt+1, lc_dt+1, d*2, d*2 from source_table settings insert_null_as_default = 0, distributed_query_wait_exception_ms=2000; -- { serverError 349 }

drop table source_table;
drop table target_table;
