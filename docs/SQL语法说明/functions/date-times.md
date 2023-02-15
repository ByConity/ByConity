---
title: "Date & Times"
slug: "date-times"
hidden: false
createdAt: "2021-07-29T12:02:04.877Z"
updatedAt: "2021-09-23T03:55:04.783Z"
tags:
  - Docs
---

> Notice:
> Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

## addDays

Function adds a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
addDays(date, interval)
addDays(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addDays(date, 1) AS add_days_with_date,
    addDays(date_time, 1) AS add_days_with_date_time
```

```plain%20text
┌─add_days_with_date─┬─add_days_with_date_time─┐
│ 2018-01-02         │ 2018-01-02 00:00:00     │
└────────────────────┴─────────────────────────┘
```

## addHours

Function adds a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
addHours(date, interval)
addHours(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addHours(date, 1) AS add_hours_with_date,
    addHours(date_time, 1) AS add_hours_with_date_time
```

```plain%20text
┌─add_hours_with_date─┬─add_hours_with_date_time─┐
│ 2018-01-01 01:00:00 │ 2018-01-01 01:00:00      │
└─────────────────────┴──────────────────────────┘
```

## addMinutes

Function adds a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
addMinutes(date, interval)
addMinutes(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addMinutes(date, 1) AS add_minutes_with_date,
    addMinutes(date_time, 1) AS add_minutes_with_date_time
```

```plain%20text
┌─add_minutes_with_date─┬─add_minutes_with_date_time─┐
│ 2018-01-01 00:01:00   │ 2018-01-01 00:01:00        │
└───────────────────────┴────────────────────────────┘
```

## addMonths

Function adds a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime. For example:

**Syntax**

```sql
addMonths(date, interval)
addMonths(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addMonths(date, 1) AS add_months_with_date,
    addMonths(date_time, 1) AS add_months_with_date_time
```

```plain%20text
┌─add_months_with_date─┬─add_months_with_date_time─┐
│ 2018-02-01           │ 2018-02-01 00:00:00       │
└──────────────────────┴───────────────────────────┘
```

## addQuarters

Function adds a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
addQuarters(date, interval)
addQuarters(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addQuarters(date, 1) AS add_quarters_with_date,
    addQuarters(date_time, 1) AS add_quarters_with_date_time
```

```plain%20text
┌─add_quarters_with_date─┬─add_quarters_with_date_time─┐
│ 2018-04-01             │ 2018-04-01 00:00:00         │
└────────────────────────┴─────────────────────────────┘
```

## addSeconds

Function adds a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime. For example:

**Syntax**

```sql
addSeconds(date, interval)
addSeconds(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addSeconds(date, 1) AS add_seconds_with_date,
    addSeconds(date_time, 1) AS add_seconds_with_date_time
```

```plain%20text
┌─add_seconds_with_date─┬─add_seconds_with_date_time─┐
│ 2018-01-01 00:00:01   │ 2018-01-01 00:00:01        │
└───────────────────────┴────────────────────────────┘
```

## addWeeks

Function adds a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
addWeeks(date, interval)
addWeeks(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addWeeks(date, 1) AS add_weeks_with_date,
    addWeeks(date_time, 1) AS add_weeks_with_date_time
```

```plain%20text
┌─add_weeks_with_date─┬─add_weeks_with_date_time─┐
│ 2018-01-08          │ 2018-01-08 00:00:00      │
└─────────────────────┴──────────────────────────┘
```

## addYears

Function adds a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime. For example:

**Syntax**

```sql
addYears(date, interval)
addYears(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addYears(date, 1) AS add_years_with_date,
    addYears(date_time, 1) AS add_years_with_date_time
```

```plain%20text
┌─add_years_with_date─┬─add_years_with_date_time─┐
│ 2019-01-01          │ 2019-01-01 00:00:00      │
└─────────────────────┴──────────────────────────┘
```

## dateDiff

Returns the difference between two dates or dates with time values.

**Syntax**

```sql
dateDiff('unit', startdate, enddate, [timezone])
```

**Arguments**

- `unit` — The type of interval for result. String.
  Possible values:
  - second
  - minute
  - hour
  - day
  - week
  - month
  - quarter
  - year
- `startdate` — The first time value to subtract (the subtrahend). Date or DateTime.
- `enddate` — The second time value to subtract from (the minuend). Date or DateTime.
- `timezone` — Timezone name (optional). If specified, it is applied to both startdate and enddate. If not specified, timezones of startdate and enddate are used. If they are not the same, the result is unspecified. String.

**Example**

```sql
SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));
```

```plain%20text
┌─dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'))─┐
│ 25                                                                                     │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## date_add

Adds the time interval or date interval to the provided date or date with time.

**Syntax**

```sql
date_add(date, value)
date_add(datetime, value [,timezone])
```

**Arguments**

- `date` — The date or date with time to which `value` is added. Date or DateTime.
- `timezone` - The timezone argument is allowed only when the 1st argument has the type DateTime

**Returned value**
Date or date with time obtained by adding `value`.

Type: Date or DateTime.

**Example**

```sql
SELECT date_add(toDate('2018-01-01'),3);
```

```plain%20text
┌─date_add(toDate('2018-01-01'), 3)─┐
│ 2018-01-04                        │
└───────────────────────────────────┘
```

```sql
SELECT date_add(toDateTime('2018-01-01 00:00:00'), 3, 'UTC');
```

```plain%20text
┌─date_add(toDateTime('2018-01-01 00:00:00'), 3, 'UTC')─┐
│ 2018-01-03 16:00:00                                   │
└───────────────────────────────────────────────────────┘
```

## date_sub

Subtracts the time interval or date interval from the provided date or date with time.

**Syntax**

```sql
date_sub(date, value)
date_sub(datetime, value [,timezone])
```

**Arguments**

- `date` — The date or date with time to which `value` is added. Date or DateTime.
- `timezone` - The timezone argument is allowed only when the 1st argument has the type DateTime

**Returned value**
Date or date with time obtained by subtracting `value`.

Type: Date or DateTime.

**Example**

```sql
SELECT date_sub(toDate('2018-01-01'),3);
```

```plain%20text
┌─date_sub(toDate('2018-01-01'), 3)─┐
│ 2017-12-29                        │
└───────────────────────────────────┘
```

```sql
SELECT date_sub(toDateTime('2018-01-01 00:00:00'), 3, 'UTC');
```

```plain%20text
┌─date_sub(toDateTime('2018-01-01 00:00:00'), 3, 'UTC')─┐
│ 2017-12-28 16:00:00                                   │
└───────────────────────────────────────────────────────┘
```

## formatDateTime

Formats a Time according to the given Format string. Format is a constant expression, so you cannot have multiple formats for a single result column.

**Syntax**

```sql
formatDateTime(Time, Format\[, Timezone\])
```

**Arguments**

- `Time` - Date/DateTime
- `Format` - Format pattern

**Replacement fields**
Using replacement fields, you can define a pattern for the resulting string. “Example” column shows formatting result for `2018-01-02 22:33:44` .

| **Placeholder** | **Description**                                        | **Example** |
| --------------- | ------------------------------------------------------ | ----------- |
| %C              | year divided by 100 and truncated to integer (00-99)   | 20          |
| %d              | day of the month, zero-padded (01-31)                  | 02          |
| %D              | Short MM/DD/YY date, equivalent to %m/%d/%y            | 01/02/18    |
| %e              | day of the month, space-padded ( 1-31)                 | 2           |
| %F              | short YYYY-MM-DD date, equivalent to %Y-%m-%d          | 2018-01-02  |
| %H              | hour in 24h format (00-23)                             | 22          |
| %I              | hour in 12h format (01-12)                             | 10          |
| %j              | day of the year (001-366)                              | 002         |
| %m              | month as a decimal number (01-12)                      | 01          |
| %M              | minute (00-59)                                         | 33          |
| %n              | new-line character (‘’)                                |             |
| %p              | AM or PM designation                                   | PM          |
| %R              | 24-hour HH:MMtime, equivalent to %H:%M                 | 22:33       |
| %S              | second (00-59)                                         | 44          |
| %t              | horizontal-tab character (’)                           |             |
| %T              | ISO8601 time format (HH:MM:SS), equivalent to %H:%M:%S | 22:33:44    |
| %u              | ISO8601 weekday as number with Monday as 1 (1-7)       | 2           |
| %V              | ISO8601 week number (01-53)                            | 01          |
| %w              | weekday as a decimal number with Sunday as 0 (0-6)     | 2           |
| %y              | Year, last two digits (00-99)                          | 18          |
| %Y              | Year                                                   | 2018        |
| %%              | a % sign                                               | %           |

**Returned value(s)**
Returns time and date values according to the determined format.

**Example**

```sql
SELECT formatDateTime(toDate('2010-01-04'), '%d');
```

```plain%20text
┌─formatDateTime(toDate('2010-01-04'), '%d')─┐
│ 04                                         │
└────────────────────────────────────────────┘
```

## from_unixtime

:::danger

NOT SUPPORT BY BYTEYARD

:::

Function converts Unix timestamp to a calendar date and a time of a day. When there is only a single argument of Integer type, it acts in the same way as toDateTime and return DateTime type.

**Example**

```sql
SELECT FROM_UNIXTIME(423543535);
```

```plain%20text
┌─FROM_UNIXTIME(423543535)─┐
│      1983-06-04 10:58:55 │
└──────────────────────────┘
```

When there are two arguments: first is an Integer or DateTime, second is a constant format string — it acts in the same way as formatDateTime and return String type.

For example:

```sql
SELECT FROM_UNIXTIME(1234334543, '%Y-%m-%d %R:%S') AS DateTime;
```

```plain%20text

┌─DateTime────────────┐

│ 2009-02-11 14:42:23 │

└─────────────────────┘

```

## now

Returns the current date and time.

**Syntax**

```sql
now()
```

**Returned value**

- Current date and time.

Type: Datetime.

**Example**

```sql
SELECT now();
```

```plain%20text
┌─now()───────────────┐
│ 2021-08-11 14:35:47 │
└─────────────────────┘
```

## subtractDays

Function subtract a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
subtractDays(date, interval)
subtractDays(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractDays(date, 1) AS subtract_days_with_date,
    subtractDays(date_time, 1) AS subtract_days_with_date_time
```

```plain%20text
┌─subtract_days_with_date─┬─subtract_days_with_date_time─┐
│ 2018-12-31              │ 2018-12-31 00:00:00          │
└─────────────────────────┴──────────────────────────────┘
```

## subtractHours

Function subtract a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
subtractHours(date, interval)
subtractHours(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractHours(date, 1) AS subtract_hours_with_date,
    subtractHours(date_time, 1) AS subtract_hours_with_date_time
```

:::danger

INCORRECT RESULT?

:::

```plain%20text
┌─subtract_hours_with_date─┬─subtract_hours_with_date_time─┐
│ 2002-11-25               │ 2018-12-31 23:00:00           │
└──────────────────────────┴───────────────────────────────┘
```

## subtractMinutes

Function subtract a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
subtractMinutes(date, interval)
subtractMinutes(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractMinutes(date, 1) AS subtract_minutes_with_date,
    subtractMinutes(date_time, 1) AS subtract_minutes_with_date_time
```

:::danger

INCORRECT RESULT?

:::

```plain%20text
┌─subtract_minutes_with_date─┬─subtract_minutes_with_date_time─┐
│ 2012-08-04                 │ 2018-12-31 23:59:00             │
└────────────────────────────┴─────────────────────────────────┘
```

## subtractMonths

Function subtract a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
subtractMonths(date, interval)
subtractMonths(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractMonths(date, 1) AS subtract_months_with_date,
    subtractMonths(date_time, 1) AS subtract_months_with_date_time
```

```plain%20text
┌─subtract_months_with_date─┬─subtract_months_with_date_time─┐
│ 2018-12-01                │ 2018-12-01 00:00:00            │
└───────────────────────────┴────────────────────────────────┘
```

## subtractQuarters

Function subtract a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
subtractQuarters(date, interval)
subtractQuarters(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractQuarters(date, 1) AS subtract_quarters_with_date,
    subtractQuarters(date_time, 1) AS subtract_quarters_with_date_time
```

```plain%20text
┌─subtract_quarters_with_date─┬─subtract_quarters_with_date_time─┐
│ 2018-10-01                  │ 2018-10-01 00:00:00              │
└─────────────────────────────┴──────────────────────────────────┘
```

## subtractSeconds

Function subtract a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
subtractSeconds(date, interval)
subtractSeconds(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractSeconds(date, 1) AS subtract_seconds_with_date,
    subtractSeconds(date_time, 1) AS subtract_seconds_with_date_time
```

:::danger

INCORRECT RESULT?

:::

```plain%20text
┌─subtract_seconds_with_date─┬─subtract_seconds_with_date_time─┐
│ 2012-10-02                 │ 2018-12-31 23:59:59             │
└────────────────────────────┴─────────────────────────────────┘
```

## subtractWeeks

Function subtract a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
subtractWeeks(date, interval)
subtractWeeks(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractWeeks(date, 1) AS subtract_weeks_with_date,
    subtractWeeks(date_time, 1) AS subtract_weeks_with_date_time
```

```plain%20text
┌─subtract_weeks_with_date─┬─subtract_weeks_with_date_time─┐
│ 2018-12-25               │ 2018-12-25 00:00:00           │
└──────────────────────────┴───────────────────────────────┘
```

## subtractYears

Function subtract a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime.

**Syntax**

```sql
subtractYears(date, interval)
subtractYears(datetime, interval)
```

**Arguments**

- `date/datetime`- Date/DateTime
- `interval` - Date/DateTime

**Returned value**

- Date/DateTime

**Example**

```sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractYears(date, 1) AS subtract_years_with_date,
    subtractYears(date_time, 1) AS subtract_years_with_date_time
```

```plain%20text
┌─subtract_years_with_date─┬─subtract_years_with_date_time─┐
│ 2018-01-01               │ 2018-01-01 00:00:00           │
└──────────────────────────┴───────────────────────────────┘
```

## timeSlot

Rounds the time to the half hour.

This function is specific to Yandex.Metrica, since half an hour is the minimum amount of time for breaking a session into two sessions if a tracking tag shows a single user’s consecutive pageviews that differ in time by strictly more than this amount. This means that tuples (the tag ID, user ID, and time slot) can be used to search for pageviews that are included in the corresponding session.

**Syntax**

```sql
timeSlot(datetime)
```

**Arguments**

- `datetime`- DateTime

**Returned value**

- DateTime

**Example**

```sql
SELECT timeSlot(toDateTime('2018-01-01 01:01:01'))
```

```plain%20text
┌─timeSlot(toDateTime('2018-01-01 01:01:01'))─┐
│ 2018-01-01 01:00:00                         │
└─────────────────────────────────────────────┘
```

## timeSlots

For a time interval starting at ‘StartTime’ and continuing for ‘Duration’ seconds, it returns an array of moments in time, consisting of points from this interval rounded down to the ‘Size’ in seconds. ‘Size’ is an optional parameter: a constant UInt32, set to 1800 by default.

This is necessary for searching for pageviews in the corresponding session.

**Syntax**

```sql
timeSlots(StartTime, Duration,[, Size])
```

**Arguments**

- `StartTime`- DateTime
- `Duration` - Duration in seconds, UInt32
- `Size` - Size of the interval.

**Returned value**

- An array of timeSlot.

**Example**

```sql
SELECT timeSlots(toDateTime('2012-01-01 12:20:00'), toUInt32(600))
```

```plain%20text
┌─timeSlots(toDateTime('2012-01-01 12:20:00'), toUInt32(600))─┐
│ [2012-01-01 12:00:00, 2012-01-01 12:30:00]                  │
└─────────────────────────────────────────────────────────────┘
```

## timezone

:::danger

Sensitive Function of ByConity

:::

Returns the timezone of the server.

**Syntax**

```sql
timezone()
```

Alias: `timezone` .

**Returned value**

- Timezone.
  Type: String.

**Example**

```sql
SELECT timezone()
```

## toDayOfMonth

Converts a date or date with time to a UInt8 number containing the number of the day of the month (1-31).

**Syntax**

```sql
toDayOfMonth(date)
toDayOfMonth(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- day of the month. UInt8

**Example**

```sql
SELECT toDayOfMonth(toDate('2021-08-12'))
```

```
┌─toDayOfMonth(toDate('2021-08-12'))─┐
│ 12                                 │
└────────────────────────────────────┘
```

```sql
SELECT toDayOfMonth(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toDayOfMonth(toDateTime('2021-08-12 00:00:00'))─┐
│ 12                                              │
└─────────────────────────────────────────────────┘
```

## toDayOfWeek

Converts a date or date with time to a UInt8 number containing the number of the day of the week (Monday is 1, and Sunday is 7).

**Syntax**

```sql
toDayOfWeek(date)
toDayOfWeek(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- day of the week. UInt8

**Example**

```sql
SELECT toDayOfWeek(toDate('2021-08-12'))
```

```
┌─toDayOfWeek(toDate('2021-08-12'))─┐
│ 4                                 │
└───────────────────────────────────┘
```

```sql
SELECT toDayOfWeek(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toDayOfWeek(toDateTime('2021-08-12 00:00:00'))─┐
│ 4                                              │
└────────────────────────────────────────────────┘
```

## toDayOfYear

Converts a date or date with time to a UInt16 number containing the number of the day of the year (1-366).

**Syntax**

```sql
toDayOfYear(date)
toDayOfYear(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- day of the year. UInt8

**Example**

```sql
SELECT toDayOfWeek(toDate('2021-08-12'))
```

```
┌─toDayOfWeek(toDate('2021-08-12'))─┐
│ 4                                 │
└───────────────────────────────────┘
```

```sql
SELECT toDayOfWeek(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toDayOfWeek(toDateTime('2021-08-12 00:00:00'))─┐
│ 4                                              │
└────────────────────────────────────────────────┘
```

## toHour

Converts a date with time to a UInt8 number containing the number of the hour in 24-hour time (0-23).

This function assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always true – even in Moscow the clocks were twice changed at a different time).

**Syntax**

```sql
toHour(datetime)
```

**Arguments**

- `datetime`- DateTime

**Returned value**

- number of the hour in 24-hour time. UInt8

**Example**

```sql
SELECT toHour(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toHour(toDateTime('2021-08-12 00:00:00'))─┐
│ 0                                         │
└───────────────────────────────────────────┘
```

## toISOWeek

Converts a date or date with time to a UInt8 number containing the ISO Week number.

**Syntax**

```sql
toISOWeek(date)
toISOWeek(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- ISO Week number. UInt8

**Example**

```sql
SELECT toISOWeek(toDate('2021-08-12'))
```

```
┌─toISOWeek(toDate('2021-08-12'))─┐
│ 32                              │
└─────────────────────────────────┘
```

```sql
SELECT toISOWeek(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toISOWeek(toDateTime('2021-08-12 00:00:00'))─┐
│ 32                                           │
└──────────────────────────────────────────────┘
```

## toISOYear

Converts a date or date with time to a UInt16 number containing the ISO Year number.

**Syntax**

```sql
toISOYear(date)
toISOYear(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- ISO Year number. UInt16

**Example**

```sql
SELECT toISOYear(toDate('2021-08-12'))
```

```
┌─toISOYear(toDate('2021-08-12'))─┐
│ 2021                            │
└─────────────────────────────────┘
```

```sql
SELECT toISOYear(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toISOYear(toDateTime('2021-08-12 00:00:00'))─┐
│ 2021                                         │
└──────────────────────────────────────────────┘
```

## toMinute

Converts a date with time to a UInt8 number containing the number of the minute of the hour (0-59).

**Syntax**

```sql
toMinute(datetime)
```

**Arguments**

- `datetime`- DateTime

**Returned value**

- minute of the hour. UInt8

**Example**

```sql
SELECT toMinute(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toMinute(toDateTime('2021-08-12 00:00:00'))─┐
│ 0                                           │
└─────────────────────────────────────────────┘
```

## toMonday

Rounds down a date or date with time to the nearest Monday.

Returns the date.

**Syntax**

```sql
toMonday(date)
toMonday(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- date to the nearest Monday. Date

**Example**

```sql
SELECT toMonday(toDate('2021-08-12'))
```

```
┌─toMonday(toDate('2021-08-12'))─┐
│ 2021-08-09                     │
└────────────────────────────────┘
```

```sql
SELECT toMonday(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toMonday(toDateTime('2021-08-12 00:00:00'))─┐
│ 2021-08-09                                  │
└─────────────────────────────────────────────┘
```

## toMonth

Converts a date or date with time to a UInt8 number containing the month number (1-12).

**Syntax**

```sql
toMonth(date)
toMonth(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- date to the nearest Monday. Date

**Example**

```sql
SELECT toMonday(toDate('2021-08-12'))
```

```
┌─toMonday(toDate('2021-08-12'))─┐
│ 2021-08-09                     │
└────────────────────────────────┘
```

```sql
SELECT toMonday(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toMonday(toDateTime('2021-08-12 00:00:00'))─┐
│ 2021-08-09                                  │
└─────────────────────────────────────────────┘
```

## toQuarter

Converts a date or date with time to a UInt8 number containing the quarter number.

**Syntax**

```sql
toQuarter(date)
toQuarter(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- quarter number. UInt8

**Example**

```sql
SELECT toQuarter(toDate('2021-08-12'))
```

```
┌─toQuarter(toDate('2021-08-12'))─┐
│ 3                               │
└─────────────────────────────────┘
```

```sql
SELECT toQuarter(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toQuarter(toDateTime('2021-08-12 00:00:00'))─┐
│ 3                                            │
└──────────────────────────────────────────────┘
```

## toRelativeDayNum

Converts a date with time or date to the number of the day, starting from a certain fixed point in the past.

**Syntax**

```sql
toRelativeDayNum(date)
toRelativeDayNum(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- number of the day, starting from a certain fixed point in the past. UInt16

**Example**

```sql
SELECT toRelativeDayNum(toDate('2021-08-12'))
```

```
┌─toRelativeDayNum(toDate('2021-08-12'))─┐
│ 18851                                  │
└────────────────────────────────────────┘
```

```sql
SELECT toRelativeDayNum(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toRelativeDayNum(toDateTime('2021-08-12 00:00:00'))─┐
│ 18851                                               │
└─────────────────────────────────────────────────────┘
```

## toRelativeHourNum

Converts a date with time or date to the number of the hour, starting from a certain fixed point in the past.

**Syntax**

```sql
toRelativeHourNum(date)
toRelativeHourNum(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- number of the hour, starting from a certain fixed point in the past. UInt32

**Example**

```sql
SELECT toRelativeHourNum(toDate('2021-08-12'))
```

```
┌─toRelativeHourNum(toDate('2021-08-12'))─┐
│ 452416                                  │
└─────────────────────────────────────────┘
```

```sql
SELECT toRelativeHourNum(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toRelativeHourNum(toDateTime('2021-08-12 00:00:00'))─┐
│ 452416                                               │
└──────────────────────────────────────────────────────┘
```

## toRelativeMinuteNum

Converts a date with time or date to the number of the minute, starting from a certain fixed point in the past.

**Syntax**

```sql
toRelativeMinuteNum(date)
toRelativeMinuteNum(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- number of the minute, starting from a certain fixed point in the past. UInt32

**Example**

```sql
SELECT toRelativeMinuteNum(toDate('2021-08-12'))
```

```
┌─toRelativeMinuteNum(toDate('2021-08-12'))─┐
│ 27144960                                  │
└───────────────────────────────────────────┘
```

```sql
SELECT toRelativeMinuteNum(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toRelativeMinuteNum(toDateTime('2021-08-12 00:00:00'))─┐
│ 27144960                                               │
└────────────────────────────────────────────────────────┘
```

## toRelativeMonthNum

Converts a date with time or date to the number of the month, starting from a certain fixed point in the past.

**Syntax**

```sql
toRelativeMonthNum(date)
toRelativeMonthNum(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- number of the month, starting from a certain fixed point in the past. UInt16

**Example**

```sql
SELECT toRelativeMonthNum(toDate('2021-08-12'))
```

```
┌─toRelativeMonthNum(toDate('2021-08-12'))─┐
│ 24260                                    │
└──────────────────────────────────────────┘
```

```sql
SELECT toRelativeMonthNum(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toRelativeMonthNum(toDateTime('2021-08-12 00:00:00'))─┐
│ 24260                                                 │
└───────────────────────────────────────────────────────┘
```

## toRelativeQuarterNum

Converts a date with time or date to the number of the quarter, starting from a certain fixed point in the past.

**Syntax**

```sql
toRelativeQuarterNum(date)
toRelativeQuarterNum(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- number of the quarter, starting from a certain fixed point in the past. UInt16

**Example**

```sql
SELECT toRelativeQuarterNum(toDate('2021-08-12'))
```

```
┌─toRelativeQuarterNum(toDate('2021-08-12'))─┐
│ 8086                                       │
└────────────────────────────────────────────┘
```

```sql
SELECT toRelativeQuarterNum(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toRelativeQuarterNum(toDateTime('2021-08-12 00:00:00'))─┐
│ 8086                                                    │
└─────────────────────────────────────────────────────────┘
```

## toRelativeSecondNum

Converts a date with time or date to the number of the second, starting from a certain fixed point in the past.

**Syntax**

```sql
toRelativeSecondNum(date)
toRelativeSecondNum(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- number of the second, starting from a certain fixed point in the past. UInt32

**Example**

```sql
SELECT toRelativeSecondNum(toDate('2021-08-12'))
```

```
┌─toRelativeSecondNum(toDate('2021-08-12'))─┐
│ 1628697600                                │
└───────────────────────────────────────────┘
```

```sql
SELECT toRelativeSecondNum(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toRelativeSecondNum(toDateTime('2021-08-12 00:00:00'))─┐
│ 1628697600                                             │
└────────────────────────────────────────────────────────┘
```

## toRelativeWeekNum

Converts a date with time or date to the number of the week, starting from a certain fixed point in the past.

**Syntax**

```sql
toRelativeWeekNum(date)
toRelativeWeekNum(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- number of the week, starting from a certain fixed point in the past. UInt16

**Example**

```sql
SELECT toRelativeWeekNum(toDate('2021-08-12'))
```

```
┌─toRelativeWeekNum(toDate('2021-08-12'))─┐
│ 2693                                    │
└─────────────────────────────────────────┘
```

```sql
SELECT toRelativeWeekNum(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toRelativeWeekNum(toDateTime('2021-08-12 00:00:00'))─┐
│ 2693                                                 │
└──────────────────────────────────────────────────────┘
```

## toRelativeYearNum

Converts a date with time or date to the number of the year, starting from a certain fixed point in the past.

**Syntax**

```sql
toRelativeYearNum(date)
toRelativeYearNum(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- number of the year, starting from a certain fixed point in the past. UInt16

**Example**

```sql
SELECT toRelativeYearNum(toDate('2021-08-12'))
```

```
┌─toRelativeYearNum(toDate('2021-08-12'))─┐
│ 2021                                    │
└─────────────────────────────────────────┘
```

```sql
SELECT toRelativeYearNum(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toRelativeYearNum(toDateTime('2021-08-12 00:00:00'))─┐
│ 2021                                                 │
└──────────────────────────────────────────────────────┘
```

## toSecond

Converts a date with time to a UInt8 number containing the number of the second in the minute (0-59).

Leap seconds are not accounted for.

**Syntax**

```sql
toSecond(datetime)
```

**Arguments**

- `datetime`- DateTime

**Returned value**

- number of the second in the minute. UInt8

**Example**

```sql
SELECT toSecond(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toSecond(toDateTime('2021-08-12 00:00:00'))─┐
│ 0                                           │
└─────────────────────────────────────────────┘
```

## toStartOfDay

Rounds down a date with time to the start of the day.

**Syntax**

```sql
toStartOfDay(datetime)
```

**Arguments**

- `datetime`- DateTime

**Returned value**

- Rounds down a date with time to the start of the day. DateTime

**Example**

```sql
SELECT toStartOfDay(toDateTime('2021-08-12 08:00:00'))
```

```
┌─toStartOfDay(toDateTime('2021-08-12 08:00:00'))─┐
│ 2021-08-12 00:00:00                             │
└─────────────────────────────────────────────────┘
```

## toStartOfFifteenMinutes

Rounds down the date with time to the start of the fifteen-minute interval.

**Syntax**

```sql
toStartOfFifteenMinutes(datetime)
```

**Arguments**

- `datetime`- DateTime

**Returned value**

- Rounds down the date with time to the start of the fifteen-minute interval. DateTime

**Example**

```sql
SELECT toStartOfFifteenMinutes(toDateTime('2021-08-12 00:10:00'))
```

```
┌─toStartOfFifteenMinutes(toDateTime('2021-08-12 00:10:00'))─┐
│ 2021-08-12 00:00:00                                        │
└────────────────────────────────────────────────────────────┘
```

## toStartOfFiveMinute

Rounds down a date with time to the start of the five-minute interval.

**Syntax**

```sql
toStartOfFiveMinute(datetime)
```

**Arguments**

- `datetime`- DateTime

**Returned value**

- Rounds down a date with time to the start of the five-minute interval. DateTime

**Example**

```sql
SELECT toStartOfFiveMinute(toDateTime('2021-08-12 00:09:00'))
```

```
┌─toStartOfFiveMinute(toDateTime('2021-08-12 00:09:00'))─┐
│ 2021-08-12 00:05:00                                    │
└────────────────────────────────────────────────────────┘
```

## toStartOfHour

Rounds down a date with time to the start of the hour.

**Syntax**

```sql
toStartOfHour(datetime)
```

**Arguments**

- `datetime`- DateTime

**Returned value**

- Rounds down a date with time to the start of the hour. DateTime

**Example**

```sql
SELECT toStartOfHour(toDateTime('2021-08-12 01:09:00'))
```

```
┌─toStartOfHour(toDateTime('2021-08-12 01:09:00'))─┐
│ 2021-08-12 01:00:00                              │
└──────────────────────────────────────────────────┘
```

## toStartOfISOYear

Rounds down a date or date with time to the first day of ISO year.

Returns the date.

**Syntax**

```sql
toStartOfISOYear(date)
toStartOfISOYear(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- Rounds down a date or date with time to the first day of ISO year. Date

**Example**

```sql
SELECT toStartOfISOYear(toDate('2021-08-12'))
```

```
┌─toStartOfISOYear(toDate('2021-08-12'))─┐
│ 2021-01-04                             │
└────────────────────────────────────────┘
```

```sql
SELECT toStartOfISOYear(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toStartOfISOYear(toDateTime('2021-08-12 00:00:00'))─┐
│ 2021-01-04                                          │
└─────────────────────────────────────────────────────┘
```

## toStartOfInterval

This is a generalization of other functions named `toStartOf*` . For example,
`toStartOfInterval(t, INTERVAL 1 year)` returns the same as `toStartOfYear(t)` ,
`toStartOfInterval(t, INTERVAL 1 month)` returns the same as `toStartOfMonth(t)` ,
`toStartOfInterval(t, INTERVAL 1 day)` returns the same as `toStartOfDay(t)` ,
`toStartOfInterval(t, INTERVAL 15 minute)` returns the same as `toStartOfFifteenMinutes(t)` etc.

**Syntax**

```sql
toStartOfInterval(time_or_data, INTERVAL x unit [, time_zone])
```

**Arguments**

- `time_or_data`- Date/DateTime
- `x unit` - x is UInt\*, unit can be second, minute, hour, day, month, year, quarter

**Returned value**

- Date/DateTime

**Example**

```sql
SELECT toStartOfInterval(toDateTime('2021-08-12 01:09:00'), INTERVAL 2 minute)
```

```
┌─toStartOfInterval(toDateTime('2021-08-12 01:09:00'), toIntervalMinute(2))─┐
│ 2021-08-12 01:08:00                                                       │
└───────────────────────────────────────────────────────────────────────────┘
```

## toStartOfMinute

Rounds down a date with time to the start of the minute.

**Syntax**

```sql
toStartOfMinute(datetime)
```

**Arguments**

- `datetime`- DateTime

**Returned value**

- Rounds down a date with time to the start of the minute. DateTime

**Example**

```sql
SELECT toStartOfMinute(toDateTime('2021-08-12 00:01:30'))
```

```
┌─toStartOfMinute(toDateTime('2021-08-12 00:01:30'))─┐
│ 2021-08-12 00:01:00                                │
└────────────────────────────────────────────────────┘
```

## toStartOfMonth

Rounds down a date or date with time to the first day of the month.

Returns the date.

**Syntax**

```sql
toStartOfMonth(date)
toStartOfMonth(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- Rounds down a date or date with time to the first day of the month. Date

**Example**

```sql
SELECT toStartOfMonth(toDate('2021-08-12'))
```

```
┌─toStartOfMonth(toDate('2021-08-12'))─┐
│ 2021-08-01                           │
└──────────────────────────────────────┘
```

```sql
SELECT toStartOfMonth(toDateTime('2021-08-12 00:01:30'))
```

```
┌─toStartOfMonth(toDateTime('2021-08-12 00:01:30'))─┐
│ 2021-08-01                                        │
└───────────────────────────────────────────────────┘
```

## toStartOfQuarter

Rounds down a date or date with time to the first day of the quarter.
The first day of the quarter is either 1 January, 1 April, 1 July, or 1 October.
Returns the date.

**Syntax**

```sql
toStartOfQuarter(date)
toStartOfQuarter(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- Rounds down a date or date with time to the first day of the quarter. Date

**Example**

```sql
SELECT toStartOfQuarter(toDate('2021-08-12'))
```

```
┌─toStartOfQuarter(toDate('2021-08-12'))─┐
│ 2021-07-01                             │
└────────────────────────────────────────┘
```

```sql
SELECT toStartOfQuarter(toDateTime('2021-08-12 00:01:30'))
```

```
┌─toStartOfQuarter(toDateTime('2021-08-12 00:01:30'))─┐
│ 2021-07-01                                          │
└─────────────────────────────────────────────────────┘
```

## toStartOfTenMinutes

Rounds down a date with time to the start of the ten-minute interval.

**Syntax**

```sql
toStartOfTenMinutes(datetime)
```

**Arguments**

- `datetime`- DateTime

**Returned value**

- Rounds down a date with time to the start of the ten-minute interval. DateTime

**Example**

```sql
SELECT toStartOfTenMinutes(toDateTime('2021-08-12 00:01:30'))
```

```
┌─toStartOfTenMinutes(toDateTime('2021-08-12 00:01:30'))─┐
│ 2021-08-12 00:00:00                                    │
└────────────────────────────────────────────────────────┘
```

## toStartOfWeek

Rounds down a date or date with time to the nearest Sunday or Monday by mode.
Returns the date.
The mode argument works exactly like the mode argument to toWeek(). For the single-argument syntax, a mode value of 0 is used.

**Syntax**

```sql
toStartOfWeek(date\[,mode\])
toStartOfWeek(datetime\[,mode\])
```

**Arguments**

- `date/datetime`- Date/DateTime
- `mode` - Refer toWeek mode.

**Returned value**

- Rounds down a date or date with time to the nearest Sunday or Monday by mode. DateTime

**Example**

```sql
SELECT toStartOfWeek(toDateTime('2021-08-12 00:01:30'))
```

```
┌─toStartOfWeek(toDateTime('2021-08-12 00:01:30'))─┐
│ 2021-08-08                                       │
└──────────────────────────────────────────────────┘
```

## toStartOfYear

Rounds down a date or date with time to the first day of the year.

Returns the date.

**Syntax**

```sql
toStartOfYear(date)
toStartOfYear(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- Rounds down a date or date with time to the first day of the year. Date

**Example**

```sql
SELECT toStartOfYear(toDateTime('2021-08-12 00:01:30'))
```

```
┌─toStartOfYear(toDateTime('2021-08-12 00:01:30'))─┐
│ 2021-01-01                                       │
└──────────────────────────────────────────────────┘
```

## toTime

Converts a date with time to a certain fixed date, while preserving the time.

**Syntax**

```sql
toTime(date)
toTime(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- DateTime

**Example**

```sql
SELECT toTime(toDateTime('2021-08-12 00:01:30'))
```

```
┌─toTypeName(toTime(toDateTime('2021-08-12 00:01:30')))─┐
│ DateTime('PRC')                                       │
└───────────────────────────────────────────────────────┘
```

## toTimeZone

Converts time or date and time to the specified time zone. The time zone is an attribute of the `Date` and `DateTime` data types. The internal value (number of seconds) of the table field or of the resultset's column does not change, the column's type changes and its string representation changes accordingly.

**Syntax**

```sql
toTimezone(value, timezone)
```

**Arguments**

- `value` — Time or date and time. Date/DateTime.
- `timezone` — Timezone for the returned value. String.

**Returned value**

- Date and time. DateTime.

**Example**

```sql
SELECT toDateTime('2019-01-01 00:00:00', 'UTC') AS time_utc,
    toTypeName(time_utc) AS type_utc,
    toInt32(time_utc) AS int32utc,
    toTimeZone(time_utc, 'Asia/Yekaterinburg') AS time_yekat,
    toTypeName(time_yekat) AS type_yekat,
    toInt32(time_yekat) AS int32yekat,
    toTimeZone(time_utc, 'US/Samoa') AS time_samoa,
    toTypeName(time_samoa) AS type_samoa,
    toInt32(time_samoa) AS int32samoa
FORMAT Vertical;
```

```plain%20text
Row 1:
──────
time_utc:   2019-01-01 00:00:00
type_utc:   DateTime('UTC')
int32utc:   1546300800
time_yekat: 2019-01-01 05:00:00
type_yekat: DateTime('Asia/Yekaterinburg')
int32yekat: 1546300800
time_samoa: 2018-12-31 13:00:00
type_samoa: DateTime('US/Samoa')
int32samoa: 1546300800
```

`toTimeZone(time_utc, 'Asia/Yekaterinburg')` changes the `DateTime('UTC')` type to `DateTime('Asia/Yekaterinburg')` . The value (Unixtimestamp) 1546300800 stays the same, but the string representation (the result of the toString() function) changes from `time_utc: 2019-01-01 00:00:00` to `time_yekat: 2019-01-01 05:00:00` .

## toWeek

This function returns the week number for date or datetime. The two-argument form of toWeek() enables you to specify whether the week starts on Sunday or Monday and whether the return value should be in the range from 0 to 53 or from 1 to 53. If the mode argument is omitted, the default mode is 0.

`toISOWeek()` is a compatibility function that is equivalent to `toWeek(date,3)` .

The following table describes how the mode argument works.

| **Mode** | **First day of week** | **Range** | **Week 1 is the first week …** |
| -------- | --------------------- | --------- | ------------------------------ |
| 0        | Sunday                | 0-53      | with a Sunday in this year     |
| 1        | Monday                | 0-53      | with 4 or more days this year  |
| 2        | Sunday                | 1-53      | with a Sunday in this year     |
| 3        | Monday                | 1-53      | with 4 or more days this year  |
| 4        | Sunday                | 0-53      | with 4 or more days this year  |
| 5        | Monday                | 0-53      | with a Monday in this year     |
| 6        | Sunday                | 1-53      | with 4 or more days this year  |
| 7        | Monday                | 1-53      | with a Monday in this year     |
| 8        | Sunday                | 1-53      | contains January 1             |
| 9        | Monday                | 1-53      | contains January 1             |

For mode values with a meaning of “with 4 or more days this year,” weeks are numbered according to ISO 8601:1988:

- If the week containing January 1 has 4 or more days in the new year, it is week 1.

- Otherwise, it is the last week of the previous year, and the next week is week 1.

For mode values with a meaning of “contains January 1”, the week contains January 1 is week 1. It does not matter how many days in the new year the week contained, even if it contained only one day.

**Syntax**

```sql
toWeek(date, [, mode][, Timezone])
```

**Arguments**

- `date` – Date or DateTime.
- `mode` – Optional parameter, Range of values is [0,9], default is 0.
- `Timezone` – Optional parameter, it behaves like any other conversion function.

**Returned value**

- week number. UInt8

**Example**

```sql
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9;
```

```plain%20text
┌─date───────┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │ 52    │ 52    │ 1     │
└────────────┴───────┴───────┴───────┘
```

## toYYYYMM

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY \* 100 + MM).

**Syntax**

```sql
toYYYYMM(date)
toYYYYMM(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- number containing the year and month number. UInt32

**Example**

```sql
SELECT toYYYYMM(toDate('2021-08-12'))
```

```
┌─toYYYYMM(toDate('2021-08-12'))─┐
│ 202108                         │
└────────────────────────────────┘
```

```sql
SELECT toYYYYMM(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toYYYYMM(toDateTime('2021-08-12 00:00:00'))─┐
│ 202108                                      │
└─────────────────────────────────────────────┘
```

## toYYYYMMDD

Converts a date or date with time to a UInt32 number containing the year and month, and day (YYYY _ 10000 + MM _ 100 + DD).

**Syntax**

```sql
toYYYYMMDD(date)
toYYYYMMDD(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- number containing the year and month, and day. UInt32

**Example**

```sql
SELECT toYYYYMMDD(toDate('2021-08-12'))
```

```
┌─toYYYYMMDD(toDate('2021-08-12'))─┐
│ 20210812                         │
└──────────────────────────────────┘
```

```sql
SELECT toYYYYMMDD(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toYYYYMMDD(toDateTime('2021-08-12 00:00:00'))─┐
│ 20210812                                      │
└───────────────────────────────────────────────┘
```

## toYYYYMMDDhhmmss

Converts a date or date with time to a UInt64 number containing the year, month, day, hour,minutes and seconds (YYYY _ 10000000000 + MM _ 100000000 + DD _ 1000000 + hh _ 10000 + mm \* 100 + ss).

**Syntax**

```sql
toYYYYMMDDhhmmss(date)
toYYYYMMDDhhmmss(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- number containing the year, month, day, hour,minutes and seconds. UInt32

**Example**

```sql
SELECT toYYYYMMDDhhmmss(toDate('2021-08-12'))
```

```
┌─toYYYYMMDDhhmmss(toDate('2021-08-12'))─┐
│ 20210812000000                         │
└────────────────────────────────────────┘
```

```sql
SELECT toYYYYMMDDhhmmss(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toYYYYMMDDhhmmss(toDateTime('2021-08-12 00:00:00'))─┐
│ 20210812000000                                      │
└─────────────────────────────────────────────────────┘
```

## toYear

Converts a date or date with time to a UInt16 number containing the year number (AD).

**Syntax**

```sql
toYear(date)
toYear(datetime)
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- Converts a date or date with time to a UInt16 number containing the year number (AD). UInt16

**Example**

```sql
SELECT toYear(toDate('2021-08-12'))
```

```
┌─toYear(toDate('2021-08-12'))─┐
│ 2021                         │
└──────────────────────────────┘
```

```sql
SELECT toYear(toDateTime('2021-08-12 00:00:00'))
```

```
┌─toYear(toDateTime('2021-08-12 00:00:00'))─┐
│ 2021                                      │
└───────────────────────────────────────────┘
```

## toYearWeek

Returns year and week for a date. The year in the result may be different from the year in the date argument for the first and the last week of the year.

The mode argument works exactly like the mode argument to toWeek(). For the single-argument syntax, a mode value of 0 is used.

`toISOYear()` is a compatibility function that is equivalent to `intDiv(toYearWeek(date,3),100)` .

**Syntax**

```sql
toYearWeek(date[,mode])
toYearWeek(datetime[,mode])
```

**Arguments**

- `date/datetime`- Date/DateTime
- `mode` - Refer toWeek mode.

**Returned value**

- year and week for a date. UInt32

**Example**

```sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9;
```

```plain%20text
┌─date───────┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┐
│ 2016-12-27 │ 201652    │ 201652    │ 201701    │
└────────────┴───────────┴───────────┴───────────┘
```

## today

Accepts zero arguments and returns the current date at one of the moments of request execution.

The same as ‘toDate(now())’.

**Syntax**

```sql
today()
```

**Returned value**

- Date.

**Example**

```sql
SELECT today()
```

## week

Alias to ToWeek

## year

Alias to toYear

## yearweek

Returns year and week for a date. The year in the result may be different from the year in the date argument for the first and the last week of the year.

The mode argument works exactly like the mode argument to toWeek(). For the single-argument syntax, a mode value of 0 is used.

`toISOYear()` is a compatibility function that is equivalent to `intDiv(toYearWeek(date,3),100)` .

**Syntax**

```sql
toYearWeek(date[,mode])
```

**Arguments**

- `date/datetime`- Date/DateTime

**Returned value**

- Rounds down a date or date with time to the first day of ISO year. Date

**Example**

```sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9;
```

```plain%20text
┌─date───────┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┐
│ 2016-12-27 │ 201652    │ 201652    │ 201701    │
└────────────┴───────────┴───────────┴───────────┘
```

## yesterday

Accepts zero arguments and returns yesterday’s date at one of the moments of request execution.

The same as ‘today() - 1’.

**Syntax**

```sql
yesterday()
```

**Returned value**

- Date.

**Example**

```sql
SELECT yesterday()
```
