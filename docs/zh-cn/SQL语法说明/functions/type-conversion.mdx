---
title: "Type Conversion"
slug: "type-conversion"
hidden: false
createdAt: "2021-07-29T12:31:12.666Z"
updatedAt: "2021-09-23T06:40:38.243Z"
tags:
  - Docs
---

> Notice:
> Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

## CAST

Converts an input value to the specified data type. Unlike the reinterpret function, `CAST` tries to present the same value using the new data type. If the conversion can not be done then an exception is raised.

**Syntax**

```sql
CAST(x, T)
CAST(x AS t)
```

<!-- TODO: CNCH not support this syntax:  x::t -->

**Arguments**

- `x` — A value to convert. May be of any type.
- `T` — The name of the target data type. String.
- `t` — The target data type.

**Returned value**

- Converted value.
  !!! note "Note"
  If the input value does not fit the bounds of the target type, the result overflows. For example, `CAST(-1, 'UInt8')` returns `255` .

**Examples**

```sql
SELECT CAST(toInt8(-1), 'UInt8') AS cast_int_to_uint, CAST(1.5 AS Decimal(3,2)) AS cast_float_to_decimal;
```

```plain%20text
┌─cast_int_to_uint─┬─cast_float_to_decimal─┐
│ 255              │ 1.50                  │
└──────────────────┴───────────────────────┘
```

```sql
SELECT
    '2016-06-15 23:00:00' AS timestamp,
    CAST(timestamp AS DateTime) AS datetime,
    CAST(timestamp AS Date) AS date,
    CAST(timestamp, 'String') AS string,
    CAST(timestamp, 'FixedString(22)') AS fixed_string;
```

```plain%20text
┌─timestamp───────────┬─datetime────────────┬─date───────┬─string──────────────┬─fixed_string────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │
└─────────────────────┴─────────────────────┴────────────┴─────────────────────┴─────────────────────┘
```

Conversion to FixedString(N) only works for arguments of type String or FixedString.

Type conversion to Nullable and back is supported.

```sql
SELECT toTypeName(number) FROM numbers(2);
```

```plain%20text
┌─toTypeName(number)─┐
│ UInt64             │
│ UInt64             │
└────────────────────┘
```

```sql
SELECT toTypeName(CAST(number, 'Nullable(UInt64)')) FROM numbers(2);
```

```plain%20text
┌─toTypeName(CAST(number, 'Nullable(UInt64)'))─┐
│ Nullable(UInt64)                             │
│ Nullable(UInt64)                             │
└──────────────────────────────────────────────┘
```

## reinterpretAsDate

These functions accept a string and interpret the bytes placed at the beginning of the string as a number in host order (little endian). If the string isn’t long enough, the functions work as if the string is padded with the necessary number of null bytes. If the string is longer than needed, the extra bytes are ignored. A date is interpreted as the number of days since the beginning of the Unix Epoch.

**Syntax**

```sql
reinterpretAsDate(fixed_string)
```

**Arguments**

- `fixed_string` — String with bytes representation.

**Returned value**

- DateTime.

**Examples**

```sql
SELECT reinterpretAsDate(reinterpretAsString(toDate('2019-01-01')));
```

```plain%20text
┌─reinterpretAsDate(reinterpretAsString(toDate('2019-01-01')))─┐
│ 2019-01-01                                                   │
└──────────────────────────────────────────────────────────────┘
```

## reinterpretAsDateTime

These functions accept a string and interpret the bytes placed at the beginning of the string as a number in host order (little endian). If the string isn’t long enough, the functions work as if the string is padded with the necessary number of null bytes. If the string is longer than needed, the extra bytes are ignored. A date is interpreted as the number of days since the beginning of the Unix Epoch, and a date with time is interpreted as the number of seconds since the beginning of the Unix Epoch.

**Syntax**

```sql
reinterpretAsDateTime(fixed_string)
```

**Arguments**

- `fixed_string` — String with bytes representation.

**Returned value**

- DateTime.

**Examples**

```sql
SELECT reinterpretAsDateTime(reinterpretAsString(toDateTime('2019-01-01 00:00:00')));
```

```plain%20text
┌─reinterpretAsDateTime(reinterpretAsString(toDateTime('2019-01-01 00:00:00')))─┐
│ 2019-01-01 00:00:00                                                           │
└───────────────────────────────────────────────────────────────────────────────┘
```

## reinterpretAsFixedString

This function accepts a number or date or date with time, and returns a FixedString containing bytes representing the corresponding value in host order (little endian). Null bytes are dropped from the end. For example, a UInt32 type value of 255 is a FixedString that is one byte long.

**Syntax**

```sql
reinterpretAsFixedString(x)
```

**Arguments**

- `x` — a number or date or date with time.

**Returned value**

- FixedString.

**Examples**

```sql
SELECT reinterpretAsFixedString(toDate('2019-01-01'));
```

```plain%20text
┌─reinterpretAsFixedString(toDate('2019-01-01'))─┐
│ �E                                             │
└────────────────────────────────────────────────┘
```

## reinterpretAsString

This function accepts a number or date or date with time, and returns a string containing bytes representing the corresponding value in host order (little endian). Null bytes are dropped from the end. For example, a UInt32 type value of 255 is a string that is one byte long.

**Syntax**

```sql
reinterpretAsString(value)
```

**Arguments**

- `value` — a number or date or date with time

**Returned value**

- String with bytes representation.

**Examples**

```sql
SELECT reinterpretAsString(toDate('2019-01-01'));
```

```plain%20text
┌─reinterpretAsString(toDate('2019-01-01'))─┐
│ �E                                        │
└───────────────────────────────────────────┘
```

## toDate

converts a String, Date, DateTime, UInt\* number to Date type.
toDate

**Syntax**

```sql
toDate(time)
```

**Arguments**

- `time` — a String, Date, DateTime, UInt\* number.

**Returned value**

- Date

**Examples**

```sql
SELECT toDate('2019-01-01');
```

```plain%20text
┌─cast_int_to_uint─┬─cast_float_to_decimal─┐
│ 255              │ 1.50                  │
└──────────────────┴───────────────────────┘
```

```sql
SELECT toDate(1);
```

```plain%20text
┌─toDate(1)──┐
│ 1970-01-02 │
└────────────┘
```

```sql
SELECT toDate(toDateTime('2019-01-01 00:00:00'));
```

```plain%20text
┌─toDate(toDateTime('2019-01-01 00:00:00'))─┐
│ 2019-01-01                                │
└───────────────────────────────────────────┘
```

## toDecimal(32|64)

Converts `value` to the Decimal data type with precision of `S` . The `value` can be a number or a string. The `S` (scale) parameter specifies the number of decimal places.

**Syntax**

```sql
toDecimal32(value, S)
toDecimal64(value, S)
```

<!-- TODO: Gateway client does not support toDecimal128(value, S) -->

**Arguments**

- `value` - can be a number or a string
- `S` (scale) parameter specifies the number of decimal places.

**Returned value**

- Decimal

**Examples**

```sql
SELECT toDecimal32(1, 2)
```

```plain%20text
┌─toDecimal32(1, 2)─┐
│ 1.00              │
└───────────────────┘
```

```sql
SELECT toDecimal32('1', 2)
```

```plain%20text
┌─toDecimal32('1', 2)─┐
│ 1.00                │
└─────────────────────┘
```

## toDecimal(32|64)OrNull

Converts an input string to a Nullable(Decimal(P,S)) data type value.

These functions should be used instead of `toDecimal*()` functions, if you prefer to get a `NULL` value instead of an exception in the event of an input value parsing error.

**Syntax**

```sql
toDecimal32OrNull(expr, S)
toDecimal64OrNull(expr, S)
```

<!-- TODO: Gateway client does not support toDecimal128OrNull(expr, S) -->

**Arguments**

- `expr` — Expression, returns a value in the String data type. ByConity expects the textual representation of the decimal number. For example, `'1.111'` .
- `S` — Scale, the number of decimal places in the resulting value.

**Returned value**
A value in the `Nullable(Decimal(P,S))` data type. The value contains:

- Number with `S` decimal places, if ByConity interprets the input string as a number.
- `NULL` , if ByConity can’t interpret the input string as a number or if the input number contains more than `S` decimal places.

**Examples**

```sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val);
```

```plain%20text
┌─val──────┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.11100 │ Nullable(Decimal(9, 5))                            │
└──────────┴────────────────────────────────────────────────────┘
```

```sql
SELECT toDecimal32OrNull(toString(-1.111), 2) AS val, toTypeName(val);
```

```plain%20text
┌─val──┬─toTypeName(toDecimal32OrNull(toString(-1.111), 2))─┐
│ ᴺᵁᴸᴸ │ Nullable(Decimal(9, 2))                            │
└──────┴────────────────────────────────────────────────────┘
```

## toDecimal(32|64)OrZero

Converts an input value to the Decimal(P,S) data type.

These functions should be used instead of `toDecimal*()` functions, if you prefer to get a `0` value instead of an exception in the event of an input value parsing error.

**Syntax**

```sql
toDecimal32OrZero( expr, S)
toDecimal64OrZero( expr, S)
```

<!-- TODO: Gateway client does not support toDecimal128OrZero( expr, S) -->

**Arguments**

- `expr` — Expression data type. ByConity expects the textual representation of the decimal number. For example, `'1.111'` .
- `S` — Scale, the number of decimal places in the resulting value.

**Returned value**
A value in the `Nullable(Decimal(P,S))` data type. The value contains:

- Number with `S` decimal places, if ByConity interprets the input string as a number.
- 0 with `S` decimal places, if ByConity can’t interpret the input string as a number or if the input number contains more than `S` decimal places.

**Example**

```sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val);
```

```plain%20text
┌─val──────┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.11100 │ Decimal(9, 5)                                      │
└──────────┴────────────────────────────────────────────────────┘
```

```sql
SELECT toDecimal32OrZero(toString(-1.111), 2) AS val, toTypeName(val);
```

```plain%20text
┌─val──┬─toTypeName(toDecimal32OrZero(toString(-1.111), 2))─┐
│ 0.00 │ Decimal(9, 2)                                      │
└──────┴────────────────────────────────────────────────────┘
```

## toFixedString

Converts a String type argument to a FixedString(N) type (a string with fixed length N). N must be a constant.

If the string has fewer bytes than N, it is padded with null bytes to the right. If the string has more bytes than N, an exception is thrown.

**Syntax**

```sql
toFixedString(s, N)
```

**Arguments**

- `s` — String.
- `N` — a constant.

**Returned value**

- FixedString

**Example**

```sql
SELECT toFixedString('1234', 5)
```

```plain%20text
┌─toFixedString('1234', 5)─┐
│ 1234                     │
└──────────────────────────┘
```

## toInt(8|16|32|64)

Converts an input value to the Int data type.

**Syntax**

```sql
toInt8(expr)
toInt16(expr)
toInt32(expr)
toInt64(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string with the decimal representation of a number. Binary, octal, and hexadecimal representations of numbers are not supported. Leading zeroes are stripped.

**Returned value**

- Integer value in the `Int8` , `Int16` , `Int32` , `Int64` data type.

Functions use [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero) , meaning they truncate fractional digits of numbers.

The behavior of functions for the NaN and Inf arguments is undefined.

When you convert a value from one to another data type, you should remember that in common case, it is an unsafe operation that can lead to a data loss. A data loss can occur if you try to fit value from a larger data type to a smaller data type, or if you convert values between different data types.

ByConity has the [same behavior as C++ programs](https://en.cppreference.com/w/cpp/language/implicit_conversion).

**Example**

```sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8);
```

```plain%20text
┌─toInt64(nan)─────────┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │ 32          │ 16            │ 8           │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toInt(8|16|32|64)OrNull

It takes an argument of type String and tries to parse it into Int (8 | 16 | 32 | 64). If failed, returns NULL.

**Syntax**

```sql
toInt8OrNull(expr)
toInt16OrNull(expr)
toInt32OrNull(expr)
toInt64OrNull(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string with the decimal representation of a number. Binary, octal, and hexadecimal representations of numbers are not supported. Leading zeroes are stripped.

**Returned value**

- Integer value in the `Int8` , `Int16` , `Int32` , `Int64` data type.

Functions use [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero) , meaning they truncate fractional digits of numbers.

The behavior of functions for the NaN and Inf arguments is undefined.

When you convert a value from one to another data type, you should remember that in common case, it is an unsafe operation that can lead to a data loss. A data loss can occur if you try to fit value from a larger data type to a smaller data type, or if you convert values between different data types.

ByConity has the [same behavior as C++ programs](https://en.cppreference.com/w/cpp/language/implicit_conversion).

**Example**

```sql
SELECT toInt64OrNull('123123'), toInt8OrNull('123qwe123');
```

```plain%20text
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## toInt(8|16|32|64)OrZero

It takes an argument of type String and tries to parse it into Int (8 | 16 | 32 | 64 ). If failed, returns 0.

**Syntax**

```sql
toInt8OrZero(expr)
toInt16OrZero(expr)
toInt32OrZero(expr)
toInt64OrZero(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string with the decimal representation of a number. Binary, octal, and hexadecimal representations of numbers are not supported. Leading zeroes are stripped.

**Returned value**

- Integer value in the `Int8` , `Int16` , `Int32` , `Int64` data type.

Functions use [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero) , meaning they truncate fractional digits of numbers.

The behavior of functions for the NaN and Inf arguments is undefined.

When you convert a value from one to another data type, you should remember that in common case, it is an unsafe operation that can lead to a data loss. A data loss can occur if you try to fit value from a larger data type to a smaller data type, or if you convert values between different data types.

ByConity has the [same behavior as C++ programs](https://en.cppreference.com/w/cpp/language/implicit_conversion).

**Example**

```sql
SELECT toInt64OrZero('123123'), toInt8OrZero('123qwe123');
```

```plain%20text
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│ 123123                  │ 0                         │
└─────────────────────────┴───────────────────────────┘
```

## toInterval(Year|Quarter|Month|Week|Day|Hour|Minute|Second)

Converts a Number type argument to an Interval data type.

**Syntax**

```sql
toIntervalSecond(number)
toIntervalMinute(number)
toIntervalHour(number)
toIntervalDay(number)
toIntervalWeek(number)
toIntervalMonth(number)
toIntervalQuarter(number)
toIntervalYear(number)
```

**Arguments**

- `number` — Duration of interval. Positive integer number.

**Returned values**

- The value in `Interval` data type.

**Example**

```sql
WITH
    toDate('2019-01-01') AS date,
    INTERVAL 1 WEEK AS interval_week,
    toIntervalWeek(1) AS interval_to_week
SELECT
    date + interval_week,
    date + interval_to_week;
```

```plain%20text
┌─plus(date, interval_week)─┬─plus(date, interval_to_week)─┐
│ 2019-01-08                │ 2019-01-08                   │
└───────────────────────────┴──────────────────────────────┘
```

## toLowCardinality

Converts input parameter to the LowCardianlity version of same data type.

To convert data from the `LowCardinality` data type use the CAST function. For example, `CAST(x as String)` .

**Syntax**

```sql
toLowCardinality(expr)
```

**Arguments**

- `expr` — Expression resulting in one of the supported data types.

**Returned values**

- Result of `expr` . Type: `LowCardinality(expr_result_type)`

**Example**

```sql
SELECT toLowCardinality('1');
```

```plain%20text
┌─toLowCardinality('1')─┐
│ 1                     │
└───────────────────────┘
```

## toString

Functions for converting between numbers, strings (but not fixed strings), dates, and dates with times.

All these functions accept one argument.

When converting to or from a string, the value is formatted or parsed using the same rules as for the TabSeparated format (and almost all other text formats). If the string can’t be parsed, an exception is thrown and the request is canceled.

When converting dates to numbers or vice versa, the date corresponds to the number of days since the beginning of the Unix epoch.

When converting dates with times to numbers or vice versa, the date with time corresponds to the number of seconds since the beginning of the Unix epoch.

The date and date-with-time formats for the toDate/toDateTime functions are defined as follows:

```plain%20text

YYYY-MM-DD

YYYY-MM-DD hh:mm:ss

```

As an exception, if converting from UInt32, Int32, UInt64, or Int64 numeric types to Date, and if the number is greater than or equal to 65536, the number is interpreted as a Unix timestamp (and not as the number of days) and is rounded to the date. This allows support for the common occurrence of writing ‘toDate(unix_timestamp)’, which otherwise would be an error and would require writing the more cumbersome ‘toDate(toDateTime(unix_timestamp))’.

Conversion between a date and date with time is performed the natural way: by adding a null time or dropping the time.

Conversion between numeric types uses the same rules as assignments between different numeric types in C++.

Additionally, the toString function of the DateTime argument can take a second String argument containing the name of the time zone. Example: `Asia/Yekaterinburg` In this case, the time is formatted according to the specified time zone.

**Syntax**

```sql
toString(value)
```

**Arguments**

- `value` — numbers, strings, dates, and datetime

**Returned values**

- String

**Example**

```sql
SELECT
    now() AS now_local,
    toString(now(), 'Asia/Yekaterinburg') AS now_yekat;
```

```plain%20text
┌─now_local───────────┬─now_yekat───────────┐
│ 2021-08-18 15:25:59 │ 2021-08-18 12:25:59 │
└─────────────────────┴─────────────────────┘
```

## toStringCutToZero

Accepts a String or FixedString argument. Returns the String with the content truncated at the first zero byte found.

**Syntax**

```sql
toStringCutToZero(s)
```

**Arguments**

- `s` — String or FixedString.

**Returned values**

- truncated string

**Example**

```sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut;
```

```plain%20text
┌─s───┬─s_cut─┐
│ foo │ foo   │
└─────┴───────┘
```

```sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut;
```

```plain%20text
┌─s──────┬─s_cut─┐
│ foobar │ foo   │
└────────┴───────┘
```

## toUInt(8|16|32|64)

Converts an input value to the UInt data type. This function family includes:

**Syntax**

```sql
toUInt8(expr)
toUInt16(expr)
toUInt32(expr)
toUInt64(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string with the decimal representation of a number. Binary, octal, and hexadecimal representations of numbers are not supported. Leading zeroes are stripped.

**Returned value**

- Integer value in the `UInt8` , `UInt16` , `UInt32` , `UInt64` data type.

Functions use [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero) , meaning they truncate fractional digits of numbers.

The behavior of functions for negative agruments and for the NaN and Inf arguments is undefined. If you pass a string with a negative number, for example `'-32'` , ByConity raises an exception.

When you convert a value from one to another data type, you should remember that in common case, it is an unsafe operation that can lead to a data loss. A data loss can occur if you try to fit value from a larger data type to a smaller data type, or if you convert values between different data types.

ByConity has the [same behavior as C++ programs](https://en.cppreference.com/w/cpp/language/implicit_conversion).

**Example**

```sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8);
```

```plain%20text
┌─toUInt64(nan)───────┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │ 4294967264    │ 16             │ 8            │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## toUnixTimestamp

For DateTime argument: converts value to the number with type UInt32 -- Unix Timestamp ( [https://en.wikipedia.org/wiki/Unix_time](https://en.wikipedia.org/wiki/Unix_time) ).

For String argument: converts the input string to the datetime according to the timezone (optional second argument, server timezone is used by default) and returns the corresponding unix timestamp.

**Syntax**

```sql
toUnixTimestamp(datetime)
toUnixTimestamp(str, [timezone])
```

**Arguments**

- `datetime` — DateTime
- `str` - datetime string
- `timezone`(optional) - timezone

**Returned value**

- Returns the unix timestamp. Type: `UInt32` .

**Example**

```sql
SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Tokyo') AS unix_timestamp
```

```plain%20text
┌─unix_timestamp─┐
│ 1509836867     │
└────────────────┘
```

```sql
SELECT toUnixTimestamp(toDateTime('2017-11-05 08:07:47', 'Asia/Tokyo')) AS unix_timestamp
```

```plain%20text
┌─unix_timestamp─┐
│ 1509836867     │
└────────────────┘
```
