---
title: "Map"
slug: "map"
hidden: true
createdAt: "2021-07-29T12:29:27.367Z"
updatedAt: "2021-07-29T12:29:27.367Z"
tags:
  - Docs
---

## map

Arranges `key:value` pairs into [Map(key, value)](https://bytedance.feishu.cn/sql-reference/data-types/map.md) data type.

**Syntax**

```sql

map(key1, value1[, key2, value2, ...])

```

**Arguments**

- `key` — The key part of the pair. [String](https://bytedance.feishu.cn/sql-reference/data-types/string.md) or [Integer](https://bytedance.feishu.cn/sql-reference/data-types/int-uint.md) .

- `value` — The value part of the pair. [String](https://bytedance.feishu.cn/sql-reference/data-types/string.md) , [Integer](https://bytedance.feishu.cn/sql-reference/data-types/int-uint.md) or [Array](https://bytedance.feishu.cn/sql-reference/data-types/array.md) .

**Returned value**

- Data structure as `key:value` pairs.

Type: [Map(key, value)](https://bytedance.feishu.cn/sql-reference/data-types/map.md) .

**Examples**

Query:

```sql

SELECT map('key1', number, 'key2', number * 2) FROM numbers(3);

```

Result:

```plain%20text

┌─map('key1', number, 'key2', multiply(number, 2))─┐

│ {'key1':0,'key2':0}                              │

│ {'key1':1,'key2':2}                              │

│ {'key1':2,'key2':4}                              │

└──────────────────────────────────────────────────┘

```

Query:

```sql

CREATE TABLE table_map (a Map(String, UInt64)) ENGINE = MergeTree() ORDER BY a;

INSERT INTO table_map SELECT map('key1', number, 'key2', number * 2) FROM numbers(3);

SELECT a['key2'] FROM table_map;

```

Result:

```plain%20text

┌─arrayElement(a, 'key2')─┐

│                       0 │

│                       2 │

│                       4 │

└─────────────────────────┘

```

**See Also**

- [Map(key, value)](https://bytedance.feishu.cn/sql-reference/data-types/map.md) data type

## tuple

A function that allows grouping multiple columns.

For columns with the types T1, T2, …, it returns a Tuple(T1, T2, …) type tuple containing these columns. There is no cost to execute the function.

Tuples are normally used as intermediate values for an argument of IN operators, or for creating a list of formal parameters of lambda functions. Tuples can’t be written to a table.

The function implements the operator `(x, y, …)` .

**Syntax**

```sql

tuple(x, y, …)

```

## tupleElement

A function that allows getting a column from a tuple.

‘N’ is the column index, starting from 1. N must be a constant. ‘N’ must be a constant. ‘N’ must be a strict postive integer no greater than the size of the tuple.

There is no cost to execute the function.

The function implements the operator `x.N` .

**Syntax**

```sql

tupleElement(tuple, n)

```
