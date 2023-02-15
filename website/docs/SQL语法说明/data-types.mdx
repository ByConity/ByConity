---
title: "Data Types"
slug: "data-types"
hidden: false
metadata:
  title: "Supported Data Types in ByConity"
  description: "ByConity can support various data types, such as Numeric Data Types, String & Binary Data Types, Date & Time Data Types, Semi-structured Data Types, etc."
createdAt: "2021-06-17T15:27:33.972Z"
updatedAt: "2022-02-28T02:26:27.494Z"
tags:
  - Docs
---

The data types provided in ByConity are adapted from ClickHouse. Visit this [page](https://clickhouse.tech/docs/en/sql-reference/data-types/) for more information on ClickHouse data types.

## Summary of Data Types

- Numeric Data Types
  - Int8, TINYINT Int16, SMALLINT Int32, INT, INTEGER Int64, BIGINT UInt8 UInt16 UInt32 UInt64
  - Float32, FLOAT Float64, DOUBLE
  - Decimal, DEC Decimal32 Decimal64 Decimal128
- String & Binary Data Types
  - String, TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT, BLOB, TINYBLOB, MEDIUMBLOB , LONGBLOB, CHAR, VARCHAR
  - FixedString, BINARY
- Date & Time Data Types
  - Date
  - DateTime, TIMESTAMP
- Semi-structured Data Types
  - Array
  - Map
  - Tuple
  - Enum8 Enum16
  - Nested
- Other Special Data Types
  - IPv4 IPv6
  - Nullable
  - UUID

## Numeric Data Types

### UInt8, UInt16, UInt32, UInt64, UInt256, Int8, Int16, Int32, Int64, Int128, Int256

Fixed-length integers, with or without a sign.

When creating tables, numeric parameters for integer numbers can be set (e.g. `TINYINT(8)`, `SMALLINT(16)`, `INT(32)`, `BIGINT(64)`), but ByConity ignores them.

**Aliases**

- `Int8` — `TINYINT`

- `Int16` — `SMALLINT`

- `Int32` — `INT`,`INTEGER`.

- `Int64` — `BIGINT`.

**Int Ranges**

- `Int8` — [-128 : 127]

- `Int16` — [-32768 : 32767]

- `Int32` — [-2147483648 : 2147483647]

- `Int64` — [-9223372036854775808 : 9223372036854775807]

- `Int128` — [-170141183460469231731687303715884105728 : 170141183460469231731687303715884105727]

- `Int256` — [-57896044618658097711785492504343953926634992332820282019728792003956564819968 : 57896044618658097711785492504343953926634992332820282019728792003956564819967]

**UInt Ranges**

- `UInt8` — [0 : 255]
- `UInt16` — [0 : 65535]
- `UInt32` — [0 : 4294967295]
- `UInt64` — [0 : 18446744073709551615]
- `UInt256` — [0 : 115792089237316195423570985008687907853269984665640564039457584007913129639935]

### Float32, Float64

**Aliases**

- `Float32` — `FLOAT`.

- `Float64` — `DOUBLE`.

When creating tables, numeric parameters for floating point numbers can be set (e.g. `FLOAT(12)`, `FLOAT(15, 22)`, `DOUBLE(12)`, `DOUBLE(4, 18)`), but ByConity ignores them.

### Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S)

Signed fixed-point numbers that keep precision during add, subtract and multiply operations. For division least significant digits are discarded (not rounded).

**Aliases**

- `Decimal` — `DEC`

**Parameters**

- P - precision. Valid range: [ 1 : 76 ]. Determines how many decimal digits number can have (including fraction).

- S - scale. Valid range: [ 0 : P ]. Determines how many decimal digits fraction can have.

Depending on P parameter value Decimal(P, S) is a synonym for:

- P from [ 1 : 9 ] - for Decimal32(S)
- P from [ 10 : 18 ] - for Decimal64(S)
- P from [ 19 : 38 ] - for Decimal128(S)

**Decimal Value Ranges**

- Decimal32(S) - ( -1 _ 10^(9 - S), 1 _ 10^(9 - S) )

- Decimal64(S) - ( -1 _ 10^(18 - S), 1 _ 10^(18 - S) )

- Decimal128(S) - ( -1 _ 10^(38 - S), 1 _ 10^(38 - S) )

For example, Decimal32(4) can contain numbers from -99999.9999 to 99999.9999 with 0.0001 step.

## String & Binary Data Types

### String

Strings of an arbitrary length. The length is not limited. The value can contain an arbitrary set of bytes, including null bytes.

The String type replaces the types VARCHAR, BLOB, CLOB, and others from other DBMSs.

When creating tables, numeric parameters for string fields can be set (e.g. `VARCHAR(255)`), but ByConity ignores them.

**Encodings**

ByConity doesn’t have the concept of encodings. Strings can contain an arbitrary set of bytes, which are stored and output as-is.

If you need to store texts, we recommend using UTF-8 encoding. At the very least, if your terminal uses UTF-8 (as recommended), you can read and write your values without making conversions.

Similarly, certain functions for working with strings have separate variations that work under the assumption that the string contains a set of bytes representing a UTF-8 encoded text.

For example, the ‘length’ function calculates the string length in bytes, while the ‘lengthUTF8’ function calculates the string length in Unicode code points, assuming that the value is UTF-8 encoded.

### Fixedstring(N)

A fixed-length string of `N` bytes (neither characters nor code points).

To declare a column of `FixedString` type, use the following syntax:

```
<column_name> FixedString(N)
```

Where `N` is a natural number.

The `FixedString` type is efficient when data has the length of precisely `N` bytes. In all other cases, it is likely to reduce efficiency.

Examples of the values that can be efficiently stored in `FixedString`-typed columns:

- The binary representation of IP addresses (`FixedString(16)` for IPv6).

- Language codes (ru_RU, en_US … ).

- Currency codes (USD, RUB … ).

- Binary representation of hashes (`FixedString(16)` for MD5, `FixedString(32)` for SHA256).

To store UUID values, use the UUID data type.

When inserting the data, ByConity:

- Complements a string with null bytes if the string contains fewer than `N` bytes.

- Throws the `Too large value for FixedString(N)` exception if the string contains more than `N` bytes.

When selecting the data, ByConity does not remove the null bytes at the end of the string. If you use the `WHERE` clause, you should add null bytes manually to match the `FixedString` value. The following example illustrates how to use the `WHERE` clause with `FixedString`.

This behaviour differs from MySQL for the `CHAR` type (where strings are padded with spaces, and the spaces are removed for output).

Note that the length of the `FixedString(N)` value is constant. The [length](https://clickhouse.tech/docs/en/sql-reference/functions/array-functions/#array_functions-length) function returns `N` even if the `FixedString(N)` value is filled only with null bytes, but the [empty](https://clickhouse.tech/docs/en/sql-reference/functions/string-functions/#empty) function returns `1` in this case.

### Date & Time Data Types

### Date

A date. Stored in two bytes as the number of days since 1970-01-01 (unsigned). Allows storing values from just after the beginning of the Unix Epoch to the upper threshold defined by a constant at the compilation stage (currently, this is until the year 2149, but the final fully-supported year is 2148).

The date value is stored without the time zone.

### DateTime

Aliases:

- `DateTime` — `TIMESTAMP`

Allows storing an instant in time, which can be expressed as a calendar date and a time of a day.

```
DateTime([timezone])
```

Supported range of values: [1970-01-01 00:00:00, 2105-12-31 23:59:59].

Resolution: 1 second.

## Semi-structured Data Types

### Array(T)

An array of `T`-type items. `T` can be any data type, including an array.

The maximum size of an array is limited to one million elements.

**Example**

You can use a function to create an array:

```
array(T)
```

You can also use square brackets.

```
[]
```

### Map(key, value)

`Map(key, value)` data type stores `key:value` pairs.

**Parameters**

\- `key` — The key part of the pair. [String](https://clickhouse.tech/docs/en/sql-reference/data-types/string/) or [Integer](https://clickhouse.tech/docs/en/sql-reference/data-types/int-uint/).

\- `value` — The value part of the pair. [String](https://clickhouse.tech/docs/en/sql-reference/data-types/string/), [Integer](https://clickhouse.tech/docs/en/sql-reference/data-types/int-uint/) or [Array](https://clickhouse.tech/docs/en/sql-reference/data-types/array/).

To get the value from an `a Map('key', 'value')` column, use `a{'key'}` syntax.

**Example**

Consider the table:

```
CREATE TABLE table_map (a Map(String, UInt64)) ENGINE=Memory;

INSERT INTO table_map VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

Select all `key2` values:

```
SELECT a{'key2'} FROM table_map;
```

Result

```
┌─mapElement(a, 'key2')─┐

│                    10  │

│                    20  │

│                    30  │

└────────────── ┘
```

### Tuple(T1,T2...)

A tuple of elements, each having an individual type.

**Example**

You can use a function to create a tuple:

```
tuple(T1, T2, ...)
```

Example of creating a tuple:

```
SELECT tuple(1,'a') AS x, toTypeName(x)
```

### Enum8, Enum 16

Enumerated type consisting of named values.

Named values must be declared as `'string' = integer` pairs. ByConity stores only numbers, but supports operations with the values through their names.

ByConity supports:

- `Enum8`: 8-bit Enum. It can contain up to 256 values enumerated in the `[-128, 127]` range.

- `Enum16`: 16-bit Enum. It can contain up to 65536 values enumerated in the `[-32768, 32767]` range.

**Example**

```
CREATE TABLE t_enum(x Enum8('hello' = 1, 'world' = 2)) ENGINE = Memory;

INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello');
```

Column `x` can only store values that are listed in the type definition: `'hello'` or `'world'`. If you try to save any other value, ByConity will raise an exception.

When you query data from the table, ByConity outputs the string values from `Enum`.

```
SELECT * FROM t_enum
```

### Nested(n1 T1, n2 T2, …)

A nested data structure is like a table inside a cell. The parameters of a nested data structure – the column names and types – are specified the same way as in a CREATE TABLE query. Each table row can correspond to any number of rows in a nested data structure.

**Example**

```
CREATE TABLE visits(Goals Nested(ID UInt32, Price Int64)) ENGINE = Memory;

INSERT INTO visits values ([1],[1]);
```

This example declares the `Goals` nested data structure, which contains data about conversions (goals reached). Each row in the ‘visits’ table can correspond to zero or any number of conversions.

Only a single nesting level is supported.

In most cases, when working with a nested data structure, its columns are specified with column names separated by a dot. These columns make up an array of matching types. All the column arrays of a single nested data structure have the same length.

```
SELECT Goals.ID, Goals.Price FROM visits WHERE length(Goals.ID) < 5 LIMIT 10;
```

## Other Special Data Types

### IPv4

`IPv4` is a domain based on `UInt32` type and serves as a typed replacement for storing IPv4 values. It provides compact storage with the human-friendly input-output format and column type information on inspection.

**Example**

```
CREATE TABLE hits (url String, from IPv4) ENGINE = Memory;

DESCRIBE TABLE hits;
```

`IPv4` domain supports custom input format as IPv4-strings:

```
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '116.253.40.133')('https://bytehouse.cloud', '108.156.83.2')('https://docs.bytehouse.cloud', '104.18.211.56');

SELECT * FROM hits;
```

### IPv6

`IPv6` is a domain based on `FixedString(16)` type and serves as a typed replacement for storing IPv6 values. It provides compact storage with the human-friendly input-output format and column type information on inspection.

**Example**

```
CREATE TABLE hits (url String, from IPv6) ENGINE = Memory;

DESCRIBE TABLE hits;
```

`IPv6` domain supports custom input as IPv6-strings:

```
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '2a02:aa08:e000:3100::2')('https://bytehouse.cloud', '2001:44c8:129:2632:33:0:252:2')('https://docs.bytehouse.cloud', '2a02:e980:1e::1');

SELECT * FROM hits;
```

### UUID

A universally unique identifier (UUID) is a 16-byte number used to identify records.

The example of UUID type value is represented below:

```
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

If you do not specify the UUID column value when inserting a new record, the UUID value is filled with zero:

```
00000000-0000-0000-0000-000000000000
```

**Example**

This example demonstrates creating a table with the UUID type column and inserting a value into the table.

```
CREATE TABLE t_uuid (x UUID, y String) ENGINE=Memory;

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1';

SELECT * FROM t_uuid
```

### Nullable(T)

Allows storing special marker (NULL) that denotes “missing value” alongside normal values allowed by `TypeName`. For example, a `Nullable(Int8)` type column can store `Int8` type values, and the rows that don’t have a value will store `NULL`.

For a `TypeName`, you can’t use composite data types [Array](https://clickhouse.tech/docs/en/sql-reference/data-types/array/) and [Tuple](https://clickhouse.tech/docs/en/sql-reference/data-types/tuple/). Composite data types can contain `Nullable` type values, such as `Array(Nullable(Int8))`.

A `Nullable` type field can’t be included in table indexes.

`NULL` is the default value for any `Nullable` type, unless specified otherwise in the ByConity server configuration.

**Example**

```
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE = Memory;

INSERT INTO t_null VALUES (1, NULL), (2, 3);

SELECT x + y FROM t_null;
```
