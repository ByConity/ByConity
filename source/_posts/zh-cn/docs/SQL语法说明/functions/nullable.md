---
title: "Nullable"
slug: "nullable"
hidden: false
createdAt: "2021-07-29T12:24:54.251Z"
updatedAt: "2021-09-23T06:35:02.401Z"
categories:
- Docs
- SQL_Syntax
tags:
- Docs
---
> Notice:
Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

## assumeNotNull
Results in an equivalent non- `Nullable` value for a Nullable type. In case the original value is `NULL` the result is undetermined. See also `ifNull` and `coalesce` functions.

**Syntax**

```sql
assumeNotNull(x)
```

**Arguments:**
- `x` — The original value. 

**Returned values**
- The original value from the non- `Nullable` type, if it is not `NULL` . 
- Implementation specific result if the original value was `NULL` . 

**Example**

```sql
CREATE TABLE IF NOT EXISTS test.functionAssumeNotNull ( x Int8,  y Nullable(Int8)) ENGINE = CnchMergeTree ORDER BY x;
INSERT INTO test.functionAssumeNotNull VALUES (1,NULL),(2,3);
SELECT * FROM test.functionAssumeNotNull;
```

```plain%20text
┌─x─┬─y────┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │ 3    │
└───┴──────┘
```

Apply the `assumeNotNull` function to the `y` column.

```sql
SELECT assumeNotNull(y),toTypeName(assumeNotNull(y))  FROM test.functionAssumeNotNull;
```

```plain%20text
┌─assumeNotNull(y)─┬─toTypeName(assumeNotNull(y))─┐
│ 0                │ Int8                         │
│ 3                │ Int8                         │
└──────────────────┴──────────────────────────────┘
```

## coalesce
Checks from left to right whether `NULL` arguments were passed and returns the first non- `NULL` argument.

**Syntax**

```sql
coalesce(x,...)
```

**Arguments**
- Any number of parameters of a non-compound type. All parameters must be compatible by data type. 

**Returned values**
- The first non- `NULL` argument.
- `NULL` , if all arguments are `NULL` . 

**Example**
Consider a list of contacts that may specify multiple ways to contact a customer.

```sql
CREATE TABLE IF NOT EXISTS test.functionCoalesce (name String, mail Nullable(String), phone Nullable(String), icq Nullable(UInt32)) ENGINE=CnchMergeTree ORDER BY name;
INSERT INTO test.functionCoalesce VALUES ('client 1', NULL, '123-45-67', 123), ('client 2', NULL, NULL, NULL);
SELECT * FROM test.functionCoalesce;
```

```plain%20text
┌─name─────┬─mail─┬─phone─────┬─icq──┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │ 123  │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴──────┘
```

The `mail` and `phone` fields are of type String, but the `icq` field is `UInt32` , so it needs to be converted to `String` .
Get the first available contact method for the customer from the contact list:


```sql
SELECT name, coalesce(mail, phone, CAST(icq,'Nullable(String)')) FROM test.functionCoalesce;
```

```plain%20text
┌─name─────┬─coalesce(mail, phone, CAST(icq, 'Nullable(String)'))─┐
│ client 1 │ 123-45-67                                            │
│ client 2 │ ᴺᵁᴸᴸ                                                 │
└──────────┴──────────────────────────────────────────────────────┘
```

## ifNull
Returns an alternative value if the main argument is `NULL` .

**Syntax**

```sql
ifNull(x,alt)
```

**Arguments:**
- `x` — The value to check for `NULL` . 
- `alt` — The value that the function returns if `x` is `NULL` . 

**Returned values**
- The value `x` , if `x` is not `NULL` . 
- The value `alt` , if `x` is `NULL` . 

**Example**

```sql
SELECT ifNull('a', 'b');
```

```plain%20text
┌─ifNull('a', 'b')─┐
│ a                │
└──────────────────┘
```

```sql
SELECT ifNull(NULL, 'b');
```

```plain%20text
┌─ifNull(NULL, 'b')─┐
│ b                 │
└───────────────────┘
```

## isNotNull
Checks whether the argument is NULL.

**Syntax**

```sql
isNotNull(x)
```

**Arguments:**
- `x` — A value with a non-compound data type. 

**Returned value**
- `0` if `x` is `NULL` . 
- `1` if `x` is not `NULL` . 

**Example**
Input table

```sql
CREATE TABLE IF NOT EXISTS test.functionIsNotNull (x UInt8, y Nullable(UInt8)) ENGINE=CnchMergeTree ORDER BY x;
INSERT INTO test.functionIsNotNull VALUES (1, NULL),(2,3);
SELECT * FROM test.functionIsNotNull;
```

```plain%20text
┌─x─┬─y────┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │ 3    │
└───┴──────┘
```

```sql
SELECT x FROM test.functionIsNotNull WHERE isNotNull(y);
```

```plain%20text
┌─x─┐
│ 2 │
└───┘
```

## isNull
Checks whether the argument is NULL.

**Syntax**

```sql
isNull(x)
```

**Arguments**
- `x` — A value with a non-compound data type. 

**Returned value**
- `1` if `x` is `NULL` . 
- `0` if `x` is not `NULL` . 

**Example**
Input table

```sql
CREATE TABLE IF NOT EXISTS test.functionIsNull (x UInt8, y Nullable(UInt8)) ENGINE=CnchMergeTree ORDER BY x;
INSERT INTO test.functionIsNull VALUES (1, NULL),(2,3);
SELECT * FROM test.functionIsNull;
```

```plain%20text
┌─x─┬─y────┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │ 3    │
└───┴──────┘
```

```sql
SELECT x FROM test.functionIsNull WHERE isNull(y);
```

```plain%20text
┌─x─┐
│ 1 │
└───┘
```

## nullIf
Returns `NULL` if the arguments are equal.

**Syntax**

```sql
nullIf(x, y)
```

**Arguments**
- `x` , `y` — Values for comparison. They must be compatible types, or ByConity will generate an exception.

**Returned values**
- `NULL` , if the arguments are equal. 
- The `x` value, if the arguments are not equal. 

**Example**

```sql
SELECT nullIf(1, 1);
```

```plain%20text
┌─nullIf(1, 1)─┐
│ ᴺᵁᴸᴸ         │
└──────────────┘
```

```sql
SELECT nullIf(1, 2);
```

```plain%20text
┌─nullIf(1, 2)─┐
│ 1            │
└──────────────┘
```

## toNullable
Converts the argument type to `Nullable` .

**Syntax**

```sql
toNullable(x)
```

**Arguments**
- `x` — The value of any non-compound type. 

**Returned value**
- The input value with a `Nullable` type. 

**Example**

```sql
SELECT toTypeName(10);
```

```plain%20text
┌─toTypeName(10)─┐
│ UInt8          │
└────────────────┘
```

```sql
SELECT toTypeName(toNullable(10));
```

```plain%20text
┌─toTypeName(toNullable(10))─┐
│ Nullable(UInt8)            │
└────────────────────────────┘
```
