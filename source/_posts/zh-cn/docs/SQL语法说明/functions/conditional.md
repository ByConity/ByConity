---
title: "Conditional"
slug: "conditional"
hidden: false
createdAt: "2021-07-29T11:58:23.803Z"
updatedAt: "2021-09-23T03:47:59.032Z"
categories:
- Docs
- SQL_Syntax
tags:
- Docs
---
> Notice:
Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByteHouse.

## multiIf
Allows you to write the CASE operator more compactly in the query.

**Syntax**
```sql
multiIf(cond_1, then_1, cond_2, then_2, ..., else)
```

**Arguments:**
- `cond_N` — The condition for the function to return `then_N` . 
- `then_N` — The result of the function when executed. 
- `else` — The result of the function if none of the conditions is met. 
The function accepts `2N+1` parameters.

**Returned values**
The function returns one of the values `then_N` or `else` , depending on the conditions `cond_N` .

**Example**
```sql
CREATE TABLE IF NOT EXISTS test.functionMultiIf (id UInt8, left Nullable(UInt8), right Nullable(UInt8)) ENGINE=CnchMergeTree ORDER BY id;
INSERT INTO test.functionMultiIf VALUES (1,NULL,4),(2,1,3),(3,2,2),(4,3,1),(5,4,NULL);
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'left is greater', left = right, 'Both equal', 'Null value') AS result
FROM test.functionMultiIf
```

```plain%20text
┌─left─┬─right─┬─result──────────┐
│ ᴺᵁᴸᴸ │ 4     │ Null value      │
│ 1    │ 3     │ left is smaller │
│ 2    │ 2     │ Both equal      │
│ 3    │ 1     │ left is greater │
│ 4    │ ᴺᵁᴸᴸ  │ Null value      │
└──────┴───────┴─────────────────┘
```