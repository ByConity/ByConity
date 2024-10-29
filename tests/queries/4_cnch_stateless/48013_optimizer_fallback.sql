set enable_optimizer_fallback=1;
SELECT multiIf(toDateTime(t, 'UTC') between toDateTime('1970-01-01 00:00:00', 'UTC') and toDateTime('2022-01-01 06:51:40', 'UTC'), 1.0, toDateTime(t, 'UTC') between toDateTime('1970-01-01 00:00:00', 'UTC') and toDateTime('2022-01-01 06:51:40', 'UTC'), 2.0, 0) FROM (SELECT '1970-01-01 00:00:00' AS t FROM system.one) Format Null;
