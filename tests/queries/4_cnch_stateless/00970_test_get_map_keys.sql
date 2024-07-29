drop table if exists test_map;

CREATE TABLE test_map (`event_date` Date, `int_map` Map(UInt32, String), `string_map` Map(String, String), `float_map` Map(Float64, String), `date_map` Map(Date, String), `date_time_map` Map(DateTime, String)) ENGINE = CnchMergeTree PARTITION BY event_date ORDER BY event_date SETTINGS index_granularity = 8192;

insert into test_map values('2001-01-01', {1:'1'}, {'1':'1'},{1.1:'1'},{'2001-01-01':'1'},{'2001-01-01 00:00:00':'1'});

select getMapKeys(currentDatabase(0), 'test_map', 'int_map');
select getMapKeys(currentDatabase(0), 'test_map', 'string_map');
select getMapKeys(currentDatabase(0), 'test_map', 'float_map');
select getMapKeys(currentDatabase(0), 'test_map', 'date_map');
select getMapKeys(currentDatabase(0), 'test_map', 'date_time_map');

select getMapKeys(currentDatabase(0), 'test_map', 'int_map', '2001.*01.*01');
select getMapKeys(currentDatabase(0), 'test_map', 'int_map', '2001.*01.*02');

drop table test_map;
