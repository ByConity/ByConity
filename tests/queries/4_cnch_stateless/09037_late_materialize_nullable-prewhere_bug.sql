drop table if exists data;
CREATE TABLE data (ts DateTime, field String, num_field Nullable(Float64)) ENGINE = CnchMergeTree() PARTITION BY ts ORDER BY ts SETTINGS enable_late_materialize = 1;
insert into data values(toDateTime('2020-05-14 02:08:00'),'some_field_value',7.);
SELECT field, countIf(num_field > 6.0) FROM data WHERE (num_field>6.0) GROUP BY field;
drop table if exists data;
