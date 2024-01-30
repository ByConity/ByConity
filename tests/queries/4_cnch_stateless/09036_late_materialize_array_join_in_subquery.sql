drop table if exists h;
create table h (EventDate Date, CounterID UInt64, WatchID UInt64) engine = CnchMergeTree order by (CounterID, EventDate) SETTINGS enable_late_materialize = 1;
insert into h values ('2020-06-10', 16671268, 1);
SELECT count() from h ARRAY JOIN [1] AS a WHERE WatchID IN (SELECT toUInt64(1)) AND (EventDate = '2020-06-10') AND (CounterID = 16671268);
drop table if exists h;
