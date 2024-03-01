set allow_suspicious_low_cardinality_types = 1;
drop table if exists tab_00718;
create table tab_00718 (a String, b LowCardinality(UInt32)) engine = CnchMergeTree order by a;
insert into tab_00718 values ('a', 1);
select *, toTypeName(b) from tab_00718;

alter table tab_00718 modify column b UInt32;
-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

select *, toTypeName(b) from tab_00718;

alter table tab_00718 modify column b LowCardinality(UInt32);
-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

select *, toTypeName(b) from tab_00718;

alter table tab_00718 modify column b StringWithDictionary;
-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

select *, toTypeName(b) from tab_00718;

alter table tab_00718 modify column b LowCardinality(UInt32);
-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

select *, toTypeName(b) from tab_00718;
alter table tab_00718 modify column b String;
-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

select *, toTypeName(b) from tab_00718;
alter table tab_00718 modify column b LowCardinality(UInt32);
-- wait task finish
SELECT sleepEachRow(3) FROM numbers(30) FORMAT Null;

select *, toTypeName(b) from tab_00718;
drop table if exists tab_00718;
