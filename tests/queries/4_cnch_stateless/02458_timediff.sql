SET dialect_type='CLICKHOUSE';
SELECT timeDiff('10:00:00'::Time(0), '12:00:00'::Time(0));
SELECT timeDiff('12:00:00'::Time(0), '10:00:00'::Time(0));
SET dialect_type='MYSQL';
SET enable_implicit_arg_type_convert=1;
SELECT timeDiff('10:00:00', '12:00:00');
SELECT timeDiff('12:00:00', '10:00:00');
