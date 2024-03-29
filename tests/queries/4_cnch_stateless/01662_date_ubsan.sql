-- tests with INT64_MAX
SELECT addMinutes(toDateTime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT addHours(toDateTime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT addWeeks(toDateTime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT addDays(toDateTime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT addYears(toDateTime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
-- tests with INT64_MIN (via overflow)
SELECT addMinutes(toDateTime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808); -- { serverError 407 }
SELECT addHours(toDateTime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808); -- { serverError 407 }
SELECT addWeeks(toDateTime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808); -- { serverError 407 }
SELECT addDays(toDateTime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808); -- { serverError 407 }
SELECT addYears(toDateTime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808); -- { serverError 407 }
-- tests with inf
SELECT addMinutes(toDateTime('2021-01-01 00:00:00', 'GMT'), inf); -- { serverError 407 }
SELECT addHours(toDateTime('2021-01-01 00:00:00', 'GMT'), inf); -- { serverError 407 }
SELECT addWeeks(toDateTime('2021-01-01 00:00:00', 'GMT'), inf); -- { serverError 407 }
SELECT addDays(toDateTime('2021-01-01 00:00:00', 'GMT'), inf); -- { serverError 407 }
SELECT addYears(toDateTime('2021-01-01 00:00:00', 'GMT'), inf); -- { serverError 407 }
