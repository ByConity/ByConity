SELECT toDate('2016-08-02 12:34:19');
SELECT toDate(toString(toDateTime('2000-01-01 00:00:00') + number)) FROM system.numbers LIMIT 3;
SELECT toDate(CAST('0000-02-07 14:28:18', 'Nullable(DateTime)'));
SELECT toDate(CAST('8888-01-17 12:53:39', 'Nullable(DateTime)'));
