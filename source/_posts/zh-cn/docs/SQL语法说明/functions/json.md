---
title: "JSON"
slug: "json"
hidden: false
createdAt: "2021-07-29T12:12:52.763Z"
updatedAt: "2021-09-23T04:06:29.273Z"
categories:
- Docs
- SQL_Syntax
tags:
- Docs
---
> Notice:
Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.


## JSONExtract
Parses a JSON and extract a value of the given ByConity data type.
This is a generalization of the previous `JSONExtract<type>` functions.
This means
`JSONExtract(..., 'String')` returns exactly the same as `JSONExtractString()` ,
`JSONExtract(..., 'Float64')` returns exactly the same as `JSONExtractFloat()` .

**Syntax**

```sql
JSONExtract(json[, indices_or_keys…], Return_type)
```

**Arguments**
- `json` – json string. 
- `indices_or_keys` - is a list of zero or more arguments each of them can be either string or integer.
    - String = access object member by key. 
    - Positive integer = access the n-th member/key from the beginning. 
    - Negative integer = access the n-th member/key from the end. 
    - Minimum index of the element is 1. Thus the element 0 does not exist.
    - You may use integers to access both JSON arrays and JSON objects.
- `Return_type` – ByConity data type. 

**Returned value**
- Extracted value of the given ByConity data type.

**Example**

```sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))')
```

```plain%20text
┌─JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))')─┐
│ (hello, [-1e+02, 2e+02, 3e+02])                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```
<!-- TODO: Gateway client returns errors for this query

```sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)')
```

```plain%20text
``` -->

```sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))')
```

```plain%20text
┌─JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))')─┐
│ [-100, ᴺᵁᴸᴸ, ᴺᵁᴸᴸ]                                                                   │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

```sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)')
```

```plain%20text
┌─JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)')─┐
│ ᴺᵁᴸᴸ                                                                              │
└───────────────────────────────────────────────────────────────────────────────────┘
```

```sql
SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8')
```

```plain%20text
┌─JSONExtract('{"passed": true}', 'passed', 'UInt8')─┐
│ 1                                                  │
└────────────────────────────────────────────────────┘
```

```sql
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)')
```

```plain%20text
┌─JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)')─┐
│ Thursday                                                                                                                                                                   │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)')
```

```plain%20text
┌─JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)')─┐
│ Friday                                                                                                                                                            │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## JSONExtractBool
Parses a JSON and extract a value. These functions are similar to `visitParam` functions.
If the value does not exist or has a wrong type, `0` will be returned.

**Syntax**

```sql
JSONExtractBool(json\[, indices_or_keys\]…) 
```

**Arguments**
- `json` – json string. 
- `indices_or_keys` - is a list of zero or more arguments each of them can be either string or integer.
    - String = access object member by key. 
    - Positive integer = access the n-th member/key from the beginning. 
    - Negative integer = access the n-th member/key from the end. 
    - Minimum index of the element is 1. Thus the element 0 does not exist.
    - You may use integers to access both JSON arrays and JSON objects.

**Returned value**
- UInt8.

**Example**

```sql
SELECT JSONExtractBool('{"passed": true}','passed')
```

```plain%20text
┌─JSONExtractBool('{"passed": true}', 'passed')─┐
│ 1                                             │
└───────────────────────────────────────────────┘
```

```sql
SELECT JSONExtractBool('{"passed": false}','passed')
```

```plain%20text
┌─JSONExtractBool('{"passed": false}', 'passed')─┐
│ 0                                              │
└────────────────────────────────────────────────┘
```

## JSONExtractFloat
Parses a JSON and extract a value. These functions are similar to `visitParam` functions.
If the value does not exist or has a wrong type, `0` will be returned.

**Syntax**

```sql
JSONExtractFloat(json\[, indices_or_keys\]…) 
```

**Arguments**
- `json` – json string. 
- `indices_or_keys` - is a list of zero or more arguments each of them can be either string or integer.
    - String = access object member by key. 
    - Positive integer = access the n-th member/key from the beginning. 
    - Negative integer = access the n-th member/key from the end. 
    - Minimum index of the element is 1. Thus the element 0 does not exist.
    - You may use integers to access both JSON arrays and JSON objects.

**Returned value**
- Float64.

**Example**

```sql
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2)
```

```plain%20text
┌─JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2)─┐
│ 2e+02                                                               │
└─────────────────────────────────────────────────────────────────────┘
```

## JSONExtractInt
Parses a JSON and extract a value. These functions are similar to `visitParam` functions.
If the value does not exist or has a wrong type, `0` will be returned.

**Syntax**

```sql
JSONExtractInt(json\[, indices_or_keys\]…) 
```

**Arguments**
- `json` – json string. 
- `indices_or_keys` - is a list of zero or more arguments each of them can be either string or integer.
    - String = access object member by key. 
    - Positive integer = access the n-th member/key from the beginning. 
    - Negative integer = access the n-th member/key from the end. 
    - Minimum index of the element is 1. Thus the element 0 does not exist.
    - You may use integers to access both JSON arrays and JSON objects.

**Returned value**
- Int64.

**Example**

```sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1)
```

```plain%20text
┌─JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1)─┐
│ -100                                                              │
└───────────────────────────────────────────────────────────────────┘
```

## JSONExtractKeysAndValues
Parses key-value pairs from a JSON where the values are of the given ByConity data type.

**Syntax**

```sql
JSONExtractKeysAndValues(json[, indices_or_keys…], Value_type)
```

**Arguments**
- `json` – json string. 
- `indices_or_keys` - is a list of zero or more arguments each of them can be either string or integer.
    - String = access object member by key. 
    - Positive integer = access the n-th member/key from the beginning. 
    - Negative integer = access the n-th member/key from the end. 
    - Minimum index of the element is 1. Thus the element 0 does not exist.
    - You may use integers to access both JSON arrays and JSON objects.
- `Value_type` - json value data type 

**Returned value**
- key-value pairs

**Example**

```sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8');
```

```plain%20text
┌─JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8')─┐
│ [(a, 5), (b, 7), (c, 11)]                                                 │
└───────────────────────────────────────────────────────────────────────────┘
```
## JSONExtractRaw
Returns a part of JSON as unparsed string.
If the part does not exist or has a wrong type, an empty string will be returned.

**Syntax**

```sql
JSONExtractRaw(json\[, indices_or_keys\]…)
```

**Arguments**
- `json` – json string. 
- `indices_or_keys` - is a list of zero or more arguments each of them can be either string or integer.
    - String = access object member by key. 
    - Positive integer = access the n-th member/key from the beginning. 
    - Negative integer = access the n-th member/key from the end. 
    - Minimum index of the element is 1. Thus the element 0 does not exist.
    - You may use integers to access both JSON arrays and JSON objects.

**Returned value**
- String

**Example**

```sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
```

```plain%20text
┌─JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b')─┐
│ [-100,200,300]                                                 │
└────────────────────────────────────────────────────────────────┘
```

## JSONExtractString
Parse a JSON and extract a string. This function is similar to `visitParamExtractString` functions.
If the value does not exist or has a wrong type, an empty string will be returned.
The value is unescaped. If unescaping failed, it returns an empty string.

**Syntax**

```sql
JSONExtractString(json\[, indices_or_keys\]…)
```

**Arguments**
- `json` – json string. 
- `indices_or_keys` - is a list of zero or more arguments each of them can be either string or integer.
    - String = access object member by key. 
    - Positive integer = access the n-th member/key from the beginning. 
    - Negative integer = access the n-th member/key from the end. 
    - Minimum index of the element is 1. Thus the element 0 does not exist.
    - You may use integers to access both JSON arrays and JSON objects.

**Returned value**
- String

**Example**

```sql
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a')
```

```plain%20text
┌─JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a')─┐
│ hello                                                             │
└───────────────────────────────────────────────────────────────────┘
```

```sql
SELECT JSONExtractString('{"abc":"\\u263a"}', 'abc')
```

```plain%20text
┌─JSONExtractString('{"abc":"\\u263a"}', 'abc')─┐
│ ☺                                             │
└───────────────────────────────────────────────┘
```

```sql
SELECT JSONExtractString('{"abc":"\\u263"}', 'abc')
```

```plain%20text
┌─JSONExtractString('{"abc":"\\u263"}', 'abc')─┐
│                                              │
└──────────────────────────────────────────────┘
```

```sql
SELECT JSONExtractString('{"abc":"hello}', 'abc')
```

```plain%20text
┌─JSONExtractString('{"abc":"hello}', 'abc')─┐
│                                            │
└────────────────────────────────────────────┘
```

## JSONExtractUInt
Parses a JSON and extract a value. These functions are similar to `visitParam` functions.
If the value does not exist or has a wrong type, `0` will be returned.

**Syntax**

```sql
JSONExtractUInt(json\[, indices_or_keys\]…) 
```

**Arguments**
- `json` – json string. 
- `indices_or_keys` - is a list of zero or more arguments each of them can be either string or integer.
    - String = access object member by key. 
    - Positive integer = access the n-th member/key from the beginning. 
    - Negative integer = access the n-th member/key from the end. 
    - Minimum index of the element is 1. Thus the element 0 does not exist.
    - You may use integers to access both JSON arrays and JSON objects.

**Returned value**
- UInt64.

**Example**

```sql
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1)
```

```plain%20text
┌─JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1)─┐
│ 300                                                                 │
└─────────────────────────────────────────────────────────────────────┘
```
## JSONHas
If the value exists in the JSON document, `1` will be returned.
If the value does not exist, `0` will be returned.

**Syntax**

```sql
JSONHas(json[, indices_or_keys]…)
```

**Arguments**
- `json` – json string. 
- `indices_or_keys` - is a list of zero or more arguments each of them can be either string or integer.
    - String = access object member by key. 
    - Positive integer = access the n-th member/key from the beginning. 
    - Negative integer = access the n-th member/key from the end. 
    - Minimum index of the element is 1. Thus the element 0 does not exist.
    - You may use integers to access both JSON arrays and JSON objects.

**Returned value**
- UInt8.

**Example**

```sql
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1)
```

```plain%20text
┌─JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b')─┐
│ 1                                                       │
└─────────────────────────────────────────────────────────┘
```

```sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4)
```

```plain%20text
┌─JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4)─┐
│ 0                                                          │
└────────────────────────────────────────────────────────────┘
```

## JSONLength
Return the length of a JSON array or a JSON object.

If the value does not exist or has a wrong type, `0` will be returned.

**Syntax**

```sql
JSONLength(json\[, indices_or_keys\]…)
```

**Arguments**
- `json` – json string. 
- `indices_or_keys` - is a list of zero or more arguments each of them can be either string or integer.
    - String = access object member by key. 
    - Positive integer = access the n-th member/key from the beginning. 
    - Negative integer = access the n-th member/key from the end. 
    - Minimum index of the element is 1. Thus the element 0 does not exist.
    - You may use integers to access both JSON arrays and JSON objects.

**Returned value**
- UInt64.

**Example**

```sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b')
```

```plain%20text
┌─JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b')─┐
│ 3                                                          │
└────────────────────────────────────────────────────────────┘
```

```sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}')
```

```plain%20text
┌─JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}')─┐
│ 2                                                     │
└───────────────────────────────────────────────────────┘
```

## JSONType
Return the type of a JSON value.

If the value does not exist, `Null` will be returned.

**Syntax**

```sql
JSONType(json\[, indices_or_keys\]…)
```
**Arguments**
- `json` – json string. 
- `indices_or_keys` - is a list of zero or more arguments each of them can be either string or integer.
    - String = access object member by key. 
    - Positive integer = access the n-th member/key from the beginning. 
    - Negative integer = access the n-th member/key from the end. 
    - Minimum index of the element is 1. Thus the element 0 does not exist.
    - You may use integers to access both JSON arrays and JSON objects.

**Returned value**
- ByConity data type.

**Example**

```sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}')
```

```plain%20text
┌─JSONType('{"a": "hello", "b": [-100, 200.0, 300]}')─┐
│ Object                                              │
└─────────────────────────────────────────────────────┘
```

```sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a')
```

```plain%20text
┌─JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a')─┐
│ String                                                   │
└──────────────────────────────────────────────────────────┘
```

```sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b')
```

```plain%20text
┌─JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b')─┐
│ Array                                                    │
└──────────────────────────────────────────────────────────┘
```

## visitParamExtractBool
Parses a true/false value. The result is UInt8.

**Syntax**

```sql
visitParamExtractBool(params, name)
```

**Arguments**
- `params` – json string. 
- `name` - json key

**Returned value**
- UInt8.

**Example**

```sql
SELECT visitParamExtractBool('{"abc":true}', 'abc')
```

```plain%20text
┌─visitParamExtractBool('{"abc":true}', 'abc')─┐
│ 1                                            │
└──────────────────────────────────────────────┘
```

```sql
SELECT visitParamExtractBool('{"abc":false}', 'abc')
```

```plain%20text
┌─visitParamExtractBool('{"abc":false}', 'abc')─┐
│ 0                                             │
└───────────────────────────────────────────────┘
```
## visitParamExtractFloat
Parses a float value. The result is Float64.

**Syntax**

```sql
visitParamExtractFloat(params, name)
```

**Arguments**
- `params` – json string. 
- `name` - json key

**Returned value**
- UInt8.

**Example**

```sql
SELECT visitParamExtractFloat('{"abc":123.0}', 'abc')
```

```plain%20text
┌─visitParamExtractFloat('{"abc":123.1}', 'abc')─┐
│ 123.1                                          │
└────────────────────────────────────────────────┘
```

## visitParamExtractInt
Parses a Int value. The result is Int64.

**Syntax**

```sql
visitParamExtractInt(params, name)
```

**Arguments**
- `params` – json string. 
- `name` - json key

**Returned value**
- Int64.

**Example**

```sql
SELECT visitParamExtractInt('{"abc":123}', 'abc')
```

```plain%20text
┌─visitParamExtractInt('{"abc":123}', 'abc')─┐
│ 123                                        │
└────────────────────────────────────────────┘
```

## visitParamExtractRaw
Returns the value of a field, including separators.

**Syntax**

```sql
visitParamExtractRaw(params, name)
```

**Arguments**
- `params` – json string. 
- `name` - json key

**Returned value**
- String.

**Example**

```sql
SELECT visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc')
```

```plain%20text
┌─visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc')─┐
│ "\n\u0000"                                          │
└─────────────────────────────────────────────────────┘
```

```sql
SELECT visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc')
```

```plain%20text
┌─visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc')─┐
│ {"def":[1,2,3]}                                        │
└────────────────────────────────────────────────────────┘
```

## visitParamExtractString
Parses the string in double quotes. The value is unescaped. If unescaping failed, it returns an empty string.

**Syntax**

```sql
visitParamExtractString(params, name)
```

**Arguments**
- `params` – json string. 
- `name` - json key

**Returned value**
- String.

**Example**
<!-- TODO: Gateway Client formatting issue

```sql
SELECT visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc')
```

```plain%20text
┌─visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc')─┐
│ "\n\u0000"                                          │
└─────────────────────────────────────────────────────┘
``` -->

```sql
SELECT visitParamExtractString('{"abc":"\\u263a"}', 'abc')
```

```plain%20text
┌─visitParamExtractString('{"abc":"\\u263a"}', 'abc')─┐
│ ☺                                                   │
└─────────────────────────────────────────────────────┘
```

```sql
SELECT visitParamExtractString('{"abc":"\\u263"}', 'abc')
```

```plain%20text
┌─visitParamExtractString('{"abc":"\\u263"}', 'abc')─┐
│                                                    │
└────────────────────────────────────────────────────┘
```

```sql
SELECT visitParamExtractString('{"abc":"hello}', 'abc')
```

```plain%20text
┌─visitParamExtractString('{"abc":"hello}', 'abc')─┐
│                                                  │
└──────────────────────────────────────────────────┘
```

There is currently no support for code points in the format `\uXXXX\uYYYY` that are not from the basic multilingual plane (they are converted to CESU-8 instead of UTF-8).

The following functions are based on [simdjson](https://github.com/lemire/simdjson) designed for more complex JSON parsing requirements. The assumption 2 mentioned above still applies.

## visitParamExtractUInt
Parses UInt64 from the value of the field named `name` . If this is a string field, it tries to parse a number from the beginning of the string. If the field does not exist, or it exists but does not contain a number, it returns 0.

**Syntax**

```sql
visitParamExtractUInt(params, name)
```

**Arguments**
- `params` – json string. 
- `name` - json key

**Returned value**
- UInt64.

**Example**

```sql
SELECT visitParamExtractUInt('{"abc":2}', 'abc')
```

```plain%20text
┌─visitParamExtractUInt('{"abc":2}', 'abc')─┐
│ 2                                         │
└───────────────────────────────────────────┘
```
## visitParamHas
Checks whether there is a field with the `name` name.

**Syntax**

```sql
visitParamHas(params, name)
```

**Arguments**
- `params` – json string. 
- `name` - json key

**Returned value**
- UInt8.

**Example**

```sql
SELECT visitParamHas('{"abc":"def"}', 'abc')
```

```plain%20text
┌─visitParamHas('{"abc":"def"}', 'abc')─┐
│ 1                                     │
└───────────────────────────────────────┘
```
