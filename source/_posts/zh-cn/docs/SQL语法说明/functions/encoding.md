---
title: "Encoding"
slug: "encoding"
hidden: false
createdAt: "2021-07-29T12:03:22.612Z"
updatedAt: "2021-09-23T03:57:43.648Z"
categories:
- Docs
- SQL_Syntax
tags:
- Docs
---
> Notice:
Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

## UUIDNumToString
Accepts a FixedString(16) value, and returns a string containing 36 characters in text format.

**Syntax**

```sql
UUIDNumToString(FixedString(16))
```

**Arguments**
- a FixedString(16) value

**Returned value**
- String.

**Example**

```sql
SELECT
    'a/<@];!~p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid
```

```plain%20text
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## UUIDStringToNum
Accepts a string containing 36 characters in the format `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` , and returns it as a set of bytes in a FixedString(16).

**Syntax**

```sql
UUIDStringToNum(String)
```

**Arguments**
- a string in uuid format

**Returned value**
- FixedString(16)

**Example**

```sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid) AS bytes
```

```plain%20text
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## bitmaskToArray
Accepts an integer. Returns an array of UInt64 numbers containing the list of powers of two that total the source number when summed. Numbers in the array are in ascending order.

**Syntax**

```sql
bitmaskToArray(num)
```

**Arguments**
- `num` – an integer

**Returned value**
- an array of UInt64 numbers containing the list of powers of two that total the source number when summed.

**Example**

```sql
SELECT bitmaskToArray(1), bitmaskToArray(3), bitmaskToArray(4)
```

```plain%20text
┌─bitmaskToArray(1)─┬─bitmaskToArray(3)─┬─bitmaskToArray(4)─┐
│ [1]               │ [1, 2]            │ [4]               │
└───────────────────┴───────────────────┴───────────────────┘
```
1 = power(2,0)
3 = power(2,0) + power(2,1)
4 = power(2,2)

## bitmaskToList
Accepts an integer. Returns a string containing the list of powers of two that total the source number when summed. They are comma-separated without spaces in text format, in ascending order.

**Syntax**

```sql
bitmaskToList(num)
```

**Arguments**
- `num` – an integer

**Returned value**
- a string containing the list of powers of two that total the source number when summed

**Example**

```sql
SELECT bitmaskToList(1), bitmaskToList(3), bitmaskToList(4)
```

```plain%20text
┌─bitmaskToList(1)─┬─bitmaskToList(3)─┬─bitmaskToList(4)─┐
│ 1                │ 1,2              │ 4                │
└──────────────────┴──────────────────┴──────────────────┘
```
1 = power(2,0)
3 = power(2,0) + power(2,1)
4 = power(2,2)

## hex
Returns a string containing the argument’s hexadecimal representation.

**Syntax**

```sql
hex(arg)
```
The function is using uppercase letters `A-F` and not using any prefixes (like `0x` ) or suffixes (like `h` ).

For integer arguments, it prints hex digits (“nibbles”) from the most significant to least significant (big endian or “human readable” order). It starts with the most significant non-zero byte (leading zero bytes are omitted) but always prints both digits of every byte even if leading digit is zero.

Values of type `Date` and `DateTime` are formatted as corresponding integers (the number of days since Epoch for Date and the value of Unix Timestamp for DateTime).

For `String` and `FixedString` , all bytes are simply encoded as two hexadecimal numbers. Zero bytes are not omitted.

<!-- Values of floating point and Decimal types are encoded as their representation in memory. As we support little endian architecture, they are encoded in little endian. Zero leading/trailing bytes are not omitted.

**Arguments**
- `arg` — A value to convert to hexadecimal. Types: String, UInts, Date or DateTime. 
<!-- TODO: FLOAT & Decimal is not support by cnch

**Returned value**
- A string with the hexadecimal representation of the argument. Type: `String` .

**Example**

```sql
SELECT hex('a'), hex(1), hex(toDate('2019-01-01')), hex(toDateTime('2019-01-01 00:00:00'))
```

```plain%20text
┌─hex('a')─┬─hex(1)─┬─hex(toDate('2019-01-01'))─┬─hex(toDateTime('2019-01-01 00:00:00'))─┐
│ 61       │ 01     │ 45E9                      │ 5C2A3D00                               │
└──────────┴────────┴───────────────────────────┴────────────────────────────────────────┘
```
<!-- TODO: NOT SUPPORT BY CNCH

```sql
SELECT hex(toFloat32(number)) as hex_presentation FROM numbers(15, 2);
```

```plain%20text
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
```

```sql
SELECT hex(toFloat64(number)) as hex_presentation FROM numbers(15, 2);
```

```plain%20text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```
## unhex
Performs the opposite operation of `hex`. It interprets each pair of hexadecimal digits (in the argument) as a number and converts it to the byte represented by the number. The return value is a binary string (BLOB).

If you want to convert the result to a number, you can use the `reverse` and `reinterpretAs<Type>` functions.

!!! note "Note"

If `unhex` is invoked from within the `gateway-client` , binary strings display using UTF-8.

**Syntax**

```sql
unhex(arg)
```

**Arguments**
- `arg` — A string containing any number of hexadecimal digits. Type: String. 
Supports both uppercase and lowercase letters `A-F` . The number of hexadecimal digits does not have to be even. If it is odd, the last digit is interpreted as the least significant half of the `00-0F` byte. If the argument string contains anything other than hexadecimal digits, some implementation-defined result is returned (an exception isn’t thrown). For a numeric argument the inverse of hex(N) is not performed by unhex().

**Returned value**
- A binary string (BLOB). Type: String.

**Example**

```sql
SELECT unhex('303132'), unhex('4D7953514C');
```

```plain%20text
┌─unhex('303132')─┬─unhex('4D7953514C')─┐
│ 012             │ MySQL               │
└─────────────────┴─────────────────────┘
```

```sql
SELECT reinterpretAsUInt64(reverse(unhex('FFF'))) AS num;
```

```plain%20text
┌─num──┐
│ 4095 │
└──────┘
```
