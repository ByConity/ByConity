---
title: "Array"
slug: "array"
hidden: false
createdAt: "2021-07-29T02:30:43.353Z"
updatedAt: "2021-09-23T08:08:05.696Z"
tags:
  - Docs
---

> Notice:
> Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

## array

Creates an array from the function arguments.

The arguments must be constants and have types that have the smallest common type. At least one argument must be passed, because otherwise it isn’t clear which type of array to create. That is, you can’t use this function to create an empty array (to do that, use the ‘emptyArray\*’ function described above).

**Syntax**

```sql
array(x1, …)
```

**Arguments**

- `x1,...` – must be constants and have types that have the smallest common type

**Returned value**

- Returns an ‘Array(T)’ type result, where ‘T’ is the smallest common type out of the passed arguments.

**Example**

```sql
SELECT array(1,2,3);
```

```plain%20text
┌─array(1, 2, 3)─┐
│ [1, 2, 3]      │
└────────────────┘
```

## arrayAll

Returns 1 if `func` returns something other than 0 for all the elements in `arr` . Otherwise, it returns 0.
Note that the `arrayAll` is a higher-order function. You can pass a lambda function to it as the first argument.

**Syntax**

```sql
arrayAll([func,] arr1, …)
```

**Arguments**

- `func` – higher-order function which must return UInt8
- `arr1,..` - arrays as input for func

**Returned value**

- Returns 1 if `func` returns something other than 0 for all the elements in `arr`

**Example**

```sql
SELECT arrayAll((x,y)->x==y,[1,2,3],[4,5,6]);
```

```plain%20text
┌─arrayAll(lambda(tuple(x, y), equals(x, y)), [1, 2, 3], [4, 5, 6])─┐
│ 0                                                                 │
└───────────────────────────────────────────────────────────────────┘
```

```sql
SELECT arrayAll((x,y)->x==y,[1,2,3],[1,2,3]);
```

```plain%20text
┌─arrayAll(lambda(tuple(x, y), equals(x, y)), [1, 2, 3], [1, 2, 3])─┐
│ 1                                                                 │
└───────────────────────────────────────────────────────────────────┘
```

## arrayConcat

Combines arrays passed as arguments.

**Syntax**

```sql
arrayConcat(arrays)
```

**Arguments**

- `arrays` – Arbitrary number of arguments of Array type.

**Returned value**

- A combined array.

**Example**

```sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

```plain%20text
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

## arrayCount

Returns the number of elements in the arr array for which func returns something other than 0. If ‘func’ is not specified, it returns the number of non-zero elements in the array.

Note that the `arrayCount` is a higher-order function. You can pass a lambda function to it as the first argument.

**Syntax**

```sql
arrayCount([func,] arr1, …)
```

**Arguments**

- `func` – higher-order function which must return UInt8
- `arr1,..` - arrays as input for func

**Returned value**

- number of elements in the arr array for which func returns something other than 0

**Example**

```sql
SELECT arrayCount((x,y)->x==y,[1,2,3],[1,5,3]);
```

```plain%20text
┌─arrayCount(lambda(tuple(x, y), equals(x, y)), [1, 2, 3], [1, 5, 3])─┐
│ 2                                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

## arrayCumSum

Returns an array of partial sums of elements in the source array (a running sum). If the `func` function is specified, then the values of the array elements are converted by this function before summing.

Note that the `arrayCumSum` is a higher-order function. You can pass a lambda function to it as the first argument.

**Syntax**

```sql
arrayCumSum([func,] arr1, …)
```

**Arguments**

- `func` – higher-order function
- `arr1,..` - arrays as input for func

**Returned value**

- An array of partial sums of elements in the source array

**Example**

```sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

```plain%20text
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

```sql
SELECT arrayCumSum(x->x+1,[1, 1, 1, 1]) AS res
```

```plain%20text
┌─res──────────┐
│ [2, 4, 6, 8] │
└──────────────┘
```

## arrayCumSumNonNegative

Same as `arrayCumSum` , returns an array of partial sums of elements in the source array (a running sum). Different `arrayCumSum` , when returned value contains a value less than zero, the value is replace with zero and the subsequent calculation is performed with zero parameters. For example:

Note that the `arraySumNonNegative` is a higher-order function. You can pass a lambda function to it as the first argument.

**Syntax**

```sql
arrayCumSumNonNegative([func,] arr1, …)
```

**Arguments**

- `func` – higher-order function
- `arr1,..` - arrays as input for func

**Returned value**

- An array of partial sums of elements in the source array

**Example**

```sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

```plain%20text
┌─res──────────┐
│ [1, 2, 0, 1] │
└──────────────┘
```

```sql
SELECT arrayCumSumNonNegative(x->x-2,[1, 1, -4, 3]) AS res
```

```plain%20text
┌─res──────────┐
│ [0, 0, 0, 1] │
└──────────────┘
```

## arrayDifference

Calculates the difference between adjacent array elements. Returns an array where the first element will be 0, the second is the difference between `a[1] - a[0]` , etc. The type of elements in the resulting array is determined by the type inference rules for subtraction (e.g. `UInt8` - `UInt8` = `Int16` ).

**Syntax**

```sql
arrayDifference(array)
```

**Arguments**

- `array` – an Array.

**Returned values**
Returns an array of differences between adjacent elements.
Type: UInt*, Int*, Float\*.

**Example**

```sql
SELECT arrayDifference([1, 2, 3, 4]);
```

```plain%20text
┌─arrayDifference([1, 2, 3, 4])─┐
│ [0, 1, 1, 1]                  │
└───────────────────────────────┘
```

Example of the overflow due to result type Int64:

```sql
SELECT arrayDifference([0, 10000000000000000000]);
```

```plain%20text
┌─arrayDifference([0, 10000000000000000000])─┐
│ [0, -8446744073709551616]                  │
└────────────────────────────────────────────┘
```

## arrayDistinct

Takes an array, returns an array containing the distinct elements only.

**Syntax**

```sql
arrayDistinct(array)
```

**Arguments**

- `array` – an Array.

**Returned values**

- Returns an array containing the distinct elements.

**Example**

```sql
SELECT arrayDistinct([1, 2, 2, 3, 1]);
```

```plain%20text
┌─arrayDistinct([1, 2, 2, 3, 1])─┐
│ [1, 2, 3]                      │
└────────────────────────────────┘
```

## arrayElement

Get the element with the index `n` from the array `arr` . `n` must be any integer type.
Indexes in an array begin from one.

Negative indexes are supported. In this case, it selects the corresponding element numbered from the end. For example, `arr[-1]` is the last item in the array.

If the index falls outside of the bounds of an array, it returns some default value (0 for numbers, an empty string for strings, etc.), except for the case with a non-constant array and a constant index 0 (in this case there will be an error `Array indices are 1-based` ).

**Syntax**

```sql
arrayElement(array, n)
```

**Arguments**

- `array` – an Array.
- `n` - an Index in the array.

**Returned values**

- Get the element with the index `n` from the array `arr`

**Example**

```sql
SELECT arrayElement([1, 2, 2, 3, 1],3);
```

```plain%20text
┌─arrayElement([1, 2, 2, 3, 1], 3)─┐
│ 2                                │
└──────────────────────────────────┘
```

## arrayEnumerate

Returns the array [1, 2, 3, … ]

This function is normally used with ARRAY JOIN. It allows counting something just once for each array after applying ARRAY JOIN.

**Syntax**

```sql
arrayEnumerate(arr)
```

**Arguments**

- `arr` – an Array.

**Returned values**

- Returns the array [1, 2, 3, … ]

**Example**

```sql
SELECT number, num FROM numbers(5) ARRAY JOIN arrayEnumerate([1,2,3]) as num
```

```
┌─number─┬─num─┐
│ 0      │ 1   │
│ 0      │ 2   │
│ 0      │ 3   │
│ 1      │ 1   │
│ 1      │ 2   │
│ 1      │ 3   │
│ 2      │ 1   │
│ 2      │ 2   │
│ 2      │ 3   │
│ 3      │ 1   │
│ 3      │ 2   │
│ 3      │ 3   │
│ 4      │ 1   │
│ 4      │ 2   │
│ 4      │ 3   │
└────────┴─────┘
```

## arrayEnumerateDense

Returns an array of the same size as the source array, indicating where each element first appears in the source array.

**Syntax**

```sql
arrayEnumerateDense(arr)
```

**Arguments**

- `arr` – an Array.

**Returned values**

- An array where each element first appears in the source array

**Example**

```sql
SELECT arrayEnumerateDense([10, 20, 10, 30])
```

```plain%20text
┌─arrayEnumerateDense([10, 20, 10, 30])─┐
│ [1, 2, 1, 3]                          │
└───────────────────────────────────────┘
```

## arrayEnumerateUniq

Returns an array the same size as the source array, indicating for each element what its position is among elements with the same value.

For example: arrayEnumerateUniq([10, 20, 10, 30]) = [1, 1, 2, 1].

This function is useful when using ARRAY JOIN and aggregation of array elements.

**Syntax**

```sql
arrayEnumerateUniq(arr, …)
```

**Arguments**

- `arr` – an Array.

**Returned values**

- Returns an array the same size as the source array, indicating for each element what its position is among elements with the same value.

**Example**

```sql
SELECT arrayEnumerateUniq([10, 20, 10, 30]) as res
```

```plain%20text
┌─res──────────┐
│ [1, 1, 2, 1] │
└──────────────┘
```

The arrayEnumerateUniq function can take multiple arrays of the same size as arguments. In this case, uniqueness is considered for tuples of elements in the same positions in all the arrays.

```sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

```plain%20text
┌─res────────────────┐
│ [1, 2, 1, 1, 2, 1] │
└────────────────────┘
```

This is necessary when using ARRAY JOIN with a nested data structure and further aggregation across multiple elements in this structure.

## arrayExists

Returns 1 if there is at least one element in `arr` for which `func` returns something other than 0. Otherwise, it returns 0.

Note that the `arrayExists` is a higher-order function.

**Syntax**

```sql
arrayExists([func,] arr1, …)
```

**Arguments**

- `func` – higher-order function which must return UInt8
- `arr1,..` - arrays as input for func

**Returned values**

- Returns 1 if there is at least one element in `arr` for which `func` returns something other than 0. Otherwise, it returns 0.

**Example**

```sql
SELECT arrayExists((x,y)->x==y,[1, 2, 2, 3, 1],[4, 5, 6, 7, 8]);
```

```plain%20text
┌─arrayExists(lambda(tuple(x, y), equals(x, y)), [1, 2, 2, 3, 1], [4, 5, 6, 7, 8])─┐
│ 0                                                                                │
└──────────────────────────────────────────────────────────────────────────────────┘
```

```sql
SELECT arrayExists((x,y)->x==y,[1, 2, 2, 3, 1],[1, 5, 6, 7, 8]);
```

```plain%20text
┌─arrayExists(lambda(tuple(x, y), equals(x, y)), [1, 2, 2, 3, 1], [1, 5, 6, 7, 8])─┐
│ 1                                                                                │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## arrayFilter

Note that the `arrayFilter` is a higher-order function. You must pass a lambda function to it as the first argument, and it can’t be omitted.
Returns an array containing only the elements in `arr1` for which `func` returns something other than 0.

**Syntax**

```sql
arrayFilter(func, arr1, …)
```

**Arguments**

- `func` – higher-order function which must return UInt8
- `arr1,..` - arrays as input for func

**Returned values**

- Returns an array containing only the elements in `arr1` for which `func` returns something other than 0.

**Example**

```sql
SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res
```

```plain%20text
┌─res───────────┐
│ ['abc World'] │
└───────────────┘
```

```sql
SELECT arrayFilter((i, x) -> x LIKE '%World%', arrayEnumerate(arr), ['Hello', 'abc World'] AS arr) AS res
```

```plain%20text
┌─res─┐
│ [2] │
└─────┘
```

[block:api-header]
{
"title": "arrayFirst"
}
[/block]
Returns the first element in the `arr1` array for which `func` returns something other than 0.
Note that the `arrayFirst` is a higher-order function. You must pass a lambda function to it as the first argument, and it can’t be omitted.

**Syntax**

```sql
arrayFirst(func, arr1, …)
```

**Arguments**

- `func` – higher-order function which must return UInt8
- `arr1,..` - arrays as input for func

**Returned values**

- Returns the first element in the `arr1` array for which `func` returns something other than 0.

**Example**

```sql
SELECT arrayFirst(x -> x LIKE '%World%', ['Hello World', 'abc World']) AS res
```

```plain%20text
┌─res─────────┐
│ Hello World │
└─────────────┘
```

## arrayFirstIndex

Returns the index of the first element in the `arr1` array for which `func` returns something other than 0.

Note that the `arrayFirstIndex` is a higher-order function. You must pass a lambda function to it as the first argument, and it can’t be omitted.

**Syntax**

```sql
arrayFirstIndex(func, arr1, …)
```

**Arguments**

- `func` – higher-order function which must return UInt8
- `arr1,..` - arrays as input for func

**Returned values**

- Returns the index of the first element in the `arr1` array for which `func` returns something other than 0.

**Example**

```sql
SELECT arrayFirstIndex(x -> x LIKE '%World%', ['Hello World', 'abc World']) AS res
```

```plain%20text
┌─res─┐
│ 1   │
└─────┘
```

## arrayIntersect

Takes multiple arrays, returns an array with elements that are present in all source arrays. Elements order in the resulting array is the same as in the first array.

**Syntax**

```sql
arrayIntersect(arr)
```

**Arguments**

- `arr1,..` - multiple arrays

**Returned values**

- Returns an array with elements that are present in all source arrays

**Example**

```sql
SELECT
    arrayIntersect([1, 2], [1, 3], [2, 3]) AS no_intersect,
    arrayIntersect([1, 2], [1, 3], [1, 4]) AS intersect
```

```plain%20text
┌─no_intersect─┬─intersect─┐
│ []           │ [1]       │
└──────────────┴───────────┘
```

## arrayJoin

This is a very unusual function.
Normal functions do not change a set of rows, but just change the values in each row (map).
Aggregate functions compress a set of rows (fold or reduce).
The ‘arrayJoin’ function takes each row and generates a set of rows (unfold).

This function takes an array as an argument, and propagates the source row to multiple rows for the number of elements in the array.

All the values in columns are simply copied, except the values in the column where this function is applied; it is replaced with the corresponding array value.

A query can use multiple `arrayJoin` functions. In this case, the transformation is performed multiple times.

Note the ARRAY JOIN syntax in the SELECT query, which provides broader possibilities.

**Syntax**

```sql
arrayJoin(arr)
```

**Arguments**

- `arr` - an Array

**Returned values**

- Propagates the source row to multiple rows for the number of elements in the array.

**Example**

```sql
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

```plain%20text
┌─dst─┬─'Hello'─┬─src───────┐
│ 1   │ Hello   │ [1, 2, 3] │
│ 2   │ Hello   │ [1, 2, 3] │
│ 3   │ Hello   │ [1, 2, 3] │
└─────┴─────────┴───────────┘
```

## arrayMap

Returns an array obtained from the original application of the `func` function to each element in the `arr` array.

Note that the `arrayMap` is a higher-order function. You must pass a lambda function to it as the first argument, and it can’t be omitted.

**Syntax**

```sql
arrayMap(func, arr1, …)
```

**Arguments**

- `func`- higher-order function
- `arr1,..` - multiple arrays

**Returned values**

- Returns an array obtained from the original application of the `func` function to each element in the `arr` array.

**Example**

```sql
SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;
```

```plain%20text
┌─res───────┐
│ [3, 4, 5] │
└───────────┘
```

The following example shows how to create a tuple of elements from different arrays:

```sql
SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) AS res
```

```plain%20text
┌─res──────────────────────┐
│ [(1, 4), (2, 5), (3, 6)] │
└──────────────────────────┘
```

## arrayPopBack

Removes the last item from the array.

**Syntax**

```sql
arrayPopBack(array)
```

**Arguments**

- `array` – Array.

**Returned values**

- `array` – An Array removes the last item from the original array.

**Example**

```sql
SELECT arrayPopBack([1, 2, 3]) AS res;
```

```plain%20text
┌─res────┐
│ [1, 2] │
└────────┘
```

## arrayPopFront

Removes the first item from the array.

**Arguments**

- `array` – Array.

**Returned values**

- `array` – An Array removes the first item from the original array.

**Example**

```sql
SELECT arrayPopFront([1, 2, 3]) AS res;
```

```plain%20text
┌─res────┐
│ [2, 3] │
└────────┘
```

## arrayPushBack

Adds one item to the end of the array.

**Syntax**

```sql
arrayPushBack(array, single_value)
```

**Arguments**

- `array` – Array.
- `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ByConity automatically sets the `single_value` type for the data type of the array. Can be `NULL` . The function adds a `NULL` element to an array, and the type of array elements converts to `Nullable` .

**Returned values**

- `array` – An Array with the new item adds to the end of original array.

**Example**

```sql
SELECT arrayPushBack(['a'], 'b') AS res, toTypeName(arrayPushBack(['a'], 'b')) as type;
```

```plain%20text
┌─res────┬─type──────────┐│ [a, b] │ Array(String) │└────────┴───────────────┘
```

```sql
SELECT arrayPushBack(['a'], NULL) AS res, toTypeName(arrayPushBack(['a'], NULL)) as type
```

```plain%20text
┌─res───────┬─type────────────────────┐
│ [a, ᴺᵁᴸᴸ] │ Array(Nullable(String)) │
└───────────┴─────────────────────────┘
```

## arrayPushFront

Adds one element to the beginning of the array.

**Syntax**

```sql
arrayPushFront(array, single_value)
```

**Arguments**

- `array` – Array.
- `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ByConity automatically sets the `single_value` type for the data type of the array. Can be `NULL` . The function adds a `NULL` element to an array, and the type of array elements converts to `Nullable` .

**Returned values**

- `array` – An Array with the new item adds to the beginning of original array.

**Example**

```sql
SELECT arrayPushFront(['b'], 'a') AS res, toTypeName(arrayPushFront(['b'], 'a')) as type;
```

```plain%20text
┌─res────┬─type──────────┐
│ [a, b] │ Array(String) │
└────────┴───────────────┘
```

```sql
SELECT arrayPushFront(['b'], NULL) AS res, toTypeName(arrayPushFront(['b'], NULL)) as type;
```

```plain%20text
┌─res───────┬─type────────────────────┐
│ [ᴺᵁᴸᴸ, b] │ Array(Nullable(String)) │
└───────────┴─────────────────────────┘
```

## arrayReduce

Applies an aggregate function to array elements and returns its result. The name of the aggregation function is passed as a string in single quotes `'max'` , `'sum'` . When using parametric aggregate functions, the parameter is indicated after the function name in parentheses `'uniqUpTo(6)'` .

**Syntax**

```sql
arrayReduce(agg_func, arr1, arr2, ..., arrN)
```

**Arguments**

- `agg_func` — The name of an aggregate function which should be a constant string.
- `arr` — Any number of array type columns as the parameters of the aggregation function.

**Returned value**

- Result of aggregate function to array elements.

**Example**

```sql
SELECT arrayReduce('max', [1, 2, 3]);
```

```plain%20text
┌─arrayReduce('max', [1, 2, 3])─┐│                             3 │└───────────────────────────────┘
```

If an aggregate function takes multiple arguments, then this function must be applied to multiple arrays of the same size.

```sql
SELECT arrayReduce('maxIf', [3, 5], [1, 0]);
```

```plain%20text
┌─arrayReduce('maxIf', [3, 5], [1, 0])─┐│                                    3 │└──────────────────────────────────────┘
```

Example with a parametric aggregate function.

```sql
SELECT arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
```

```plain%20text
┌─arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])─┐│                                                           4 │└─────────────────────────────────────────────────────────────┘
```

## arrayResize

Changes the length of the array.

**Syntax**

```sql
arrayResize(array, size[, extender])
```

**Arguments**

- `array` — Array.
- `size` — Required length of the array.
  - If `size` is less than the original size of the array, the array is truncated from the right.
  - If `size` is larger than the initial size of the array, the array is extended to the right with `extender` values or default values for the data type of the array items.
- `extender` — Value for extending an array. Can be `NULL` .

**Returned value**

- An array of length `size` .

**Examples of calls**

```sql
SELECT arrayResize([1], 3);
```

```plain%20text
┌─arrayResize([1], 3)─┐
│ [1, 0, 0]           │
└─────────────────────┘
```

```sql
SELECT arrayResize([1], 3, NULL);
```

```plain%20text
┌─arrayResize([1], 3, NULL)─┐
│ [1, ᴺᵁᴸᴸ, ᴺᵁᴸᴸ]           │
└───────────────────────────┘
```

## arrayReverse

Returns an array of the same size as the original array containing the elements in reverse order.

**Syntax**

```sql
arrayReverse(array)
```

**Arguments**

- `array` — Array.

**Returned value**

- Reversed orginal array.

**Examples**

```sql
SELECT arrayReverse([1, 2, 3])
```

```plain%20text
┌─arrayReverse([1, 2, 3])─┐
│ [3, 2, 1]               │
└─────────────────────────┘
```

## arrayReverseSort

Sorts the elements of the `arr` array in descending order. If the `func` function is specified, `arr` is sorted according to the result of the `func` function applied to the elements of the array, and then the sorted array is reversed. If `func` accepts multiple arguments, the `arrayReverseSort` function is passed several arrays that the arguments of `func` will correspond to. Detailed examples are shown at the end of `arrayReverseSort` description.

Note that the `arrayReverseSort` is a higher-order function. You can pass a lambda function to it as the first argument. Example is shown below.

**Syntax**

```sql
arrayReverseSort([func,] arr, …)
```

**Arguments**

- `func` - sort function.
- `array` — Array.

**Returned value**

- Reversed sorted array.

**Examples**
Example of integer values sorting:

```sql
SELECT arrayReverseSort([1, 3, 3, 0]);
```

```plain%20text
┌─arrayReverseSort([1, 3, 3, 0])─┐
│ [3, 3, 1, 0]                   │
└────────────────────────────────┘
```

Example of string values sorting:

```sql
SELECT arrayReverseSort(['hello', 'world', '!']);
```

```plain%20text
┌─arrayReverseSort(['hello', 'world', '!'])─┐
│ [world, hello, !]                         │
└───────────────────────────────────────────┘
```

Consider the following sorting order for the `NULL` , `NaN` and `Inf` values:

```sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]) as res;
```

```plain%20text
┌─res─────────────────────────────────────────────────────────────┐
│ [+Inf, 3e+00, 2e+00, 1e+00, -4e+00, -Inf, NaN, NaN, ᴺᵁᴸᴸ, ᴺᵁᴸᴸ] │
└─────────────────────────────────────────────────────────────────┘
```

- `Inf` values are first in the array.
- `NULL` values are last in the array.
- `NaN` values are right before `NULL` .
- `-Inf` values are right before `NaN` .

```sql
SELECT arrayReverseSort((x) -> -x, [1, 2, 3]) as res;
```

```plain%20text
┌─res───────┐
│ [1, 2, 3] │
└───────────┘
```

The array is sorted in the following way:

1. At first, the source array ([1, 2, 3]) is sorted according to the result of the lambda function applied to the elements of the array. The result is an array [3, 2, 1].
2. Array that is obtained on the previous step, is reversed. So, the final result is [1, 2, 3].

The lambda function can accept multiple arguments. In this case, you need to pass the `arrayReverseSort` function several arrays of identical length that the arguments of lambda function will correspond to. The resulting array will consist of elements from the first input array; elements from the next input array(s) specify the sorting keys. For example:

```sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

```plain%20text
┌─res────────────┐
│ [hello, world] │
└────────────────┘
```

In this example, the array is sorted in the following way:

1. At first, the source array ([‘hello’, ‘world’]) is sorted according to the result of the lambda function applied to the elements of the arrays. The elements that are passed in the second array ([2, 1]), define the sorting keys for corresponding elements from the source array. The result is an array [‘world’, ‘hello’].
2. Array that was sorted on the previous step, is reversed. So, the final result is [‘hello’, ‘world’].

Other examples are shown below.

```sql
SELECT arrayReverseSort((x, y) -> y, [4, 3, 5], ['a', 'b', 'c']) AS res;
```

```plain%20text
┌─res───────┐
│ [5, 3, 4] │
└───────────┘
```

```sql
SELECT arrayReverseSort((x, y) -> -y, [4, 3, 5], [1, 2, 3]) AS res;
```

```plain%20text
┌─res───────┐
│ [4, 3, 5] │
└───────────┘
```

## arraySlice

Returns a slice of the array.

**Syntax**

```sql
arraySlice(array, offset[, length])
```

**Arguments**

- `array` – Array of data.
- `offset` – Indent from the edge of the array. A positive value indicates an offset on the left, and a negative value is an indent on the right. Numbering of the array items begins with 1.
- `length` – The length of the required slice. If you specify a negative value, the function returns an open slice `[offset, array_length - length)` . If you omit the value, the function returns the slice `[offset, the_end_of_array]` .

**Returned value**

- Slice of array.

**Example**

```sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res;
```

```plain%20text
┌─res──────────┐
│ [2, ᴺᵁᴸᴸ, 4] │
└──────────────┘
```

Array elements set to `NULL` are handled as normal values.

## arraySort

Sorts the elements of the `arr` array in ascending order. If the `func` function is specified, sorting order is determined by the result of the `func` function applied to the elements of the array. If `func` accepts multiple arguments, the `arraySort` function is passed several arrays that the arguments of `func` will correspond to. Detailed examples are shown at the end of `arraySort` description.

Note that `arraySort` is a higher-order function. You can pass a lambda function to it as the first argument. In this case, sorting order is determined by the result of the lambda function applied to the elements of the array.

To improve sorting efficiency, the [Schwartzian transform](https://en.wikipedia.org/wiki/Schwartzian_transform) is used.
**Syntax**

```sql
arraySort([func,] arr, …)
```

**Arguments**

- `func` - sort function.
- `array` — Array.

**Returned value**

- Sorted array.

**Example**

```sql
SELECT arraySort([1, 3, 3, 0]);
```

```plain%20text
┌─arraySort([1, 3, 3, 0])─┐
│ [0, 1, 3, 3]            │
└─────────────────────────┘
```

Example of string values sorting:

```sql
SELECT arraySort(['hello', 'world', '!']);
```

```plain%20text
┌─arraySort(['hello', 'world', '!'])─┐
│ [!, hello, world]                  │
└────────────────────────────────────┘
```

Consider the following sorting order for the `NULL` , `NaN` and `Inf` values:

```sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);
```

```plain%20text
┌─arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf])───────┐│ [-Inf, -4e+00, 1e+00, 2e+00, 3e+00, +Inf, NaN, NaN, ᴺᵁᴸᴸ, ᴺᵁᴸᴸ] │└─────────────────────────────────────────────────────────────────┘
```

- `-Inf` values are first in the array.
- `NULL` values are last in the array.
- `NaN` values are right before `NULL` .
- `Inf` values are right before `NaN` .

```sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```

```plain%20text
┌─res───────┐
│ [3, 2, 1] │
└───────────┘
```

For each element of the source array, the lambda function returns the sorting key, that is, [1 –\> -1, 2 –\> -2, 3 –\> -3]. Since the `arraySort` function sorts the keys in ascending order, the result is [3, 2, 1]. Thus, the `(x) –> -x` lambda function sets the descending order in a sorting.

The lambda function can accept multiple arguments. In this case, you need to pass the `arraySort` function several arrays of identical length that the arguments of lambda function will correspond to. The resulting array will consist of elements from the first input array; elements from the next input array(s) specify the sorting keys. For example:

```sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

```plain%20text
┌─res────────────┐│ [world, hello] │└────────────────┘
```

Here, the elements that are passed in the second array ([2, 1]) define a sorting key for the corresponding element from the source array ([‘hello’, ‘world’]), that is, [‘hello’ –\> 2, ‘world’ –\> 1]. Since the lambda function does not use `x` , actual values of the source array do not affect the order in the result. So, ‘hello’ will be the second element in the result, and ‘world’ will be the first.

Other examples are shown below.

```sql
SELECT arraySort((x, y) -> y, [0, 1, 2], ['c', 'b', 'a']) as res;
```

```plain%20text
┌─res───────┐
│ [2, 1, 0] │
└───────────┘
```

```sql
SELECT arraySort((x, y) -> -y, [0, 1, 2], [1, 2, 3]) as res;
```

```plain%20text
┌─res───────┐
│ [2, 1, 0] │
└───────────┘
```

## arraySum

Returns the sum of elements in the source array.

If the `func` function is specified, returns the sum of elements converted by this function.

Note that the `arraySum` is a higher-order function. You can pass a lambda function to it as the first argument.

**Syntax**

```sql
arraySum([func,] arr)
```

**Arguments**

- `func` — higher-order function.
- `arr` — Array.

**Returned value**

- The sum of the function values (or the array sum).
  Type:
  - for decimal numbers in source array (or for converted values, if `func` is specified) Decimal128 Float64
  - for numeric unsigned UInt64
  - and for numeric signed Int64

**Examples**

```sql
SELECT arraySum([2, 3]) AS res;
```

```plain%20text
┌─res─┐
│ 5   │
└─────┘
```

```sql
SELECT arraySum(x -> x*x, [2, 3]) AS res;
```

```plain%20text
┌─res─┐
│ 13  │
└─────┘
```

## arrayUniq

If one argument is passed, it counts the number of different elements in the array.

If multiple arguments are passed, it counts the number of different tuples of elements at corresponding positions in multiple arrays.

If you want to get a list of unique items in an array, you can use `arrayReduce(‘groupUniqArray’, arr)` .

**Syntax**

```sql
arrayUniq(arr, …)
```

**Arguments**

- `arr` — Array.

**Examples**

```sql
SELECT arrayUniq([2, 3]) AS res;
```

```plain%20text
┌─res─┐
│ 2   │
└─────┘
```

```sql
SELECT arrayUniq([2, 3, 3], [1, 2, 3]) AS res
```

```plain%20text
┌─res─┐
│ 3   │
└─────┘
```

There are three different tuples (2,1),(3,2),(3,3).

## countEqual

Returns the number of elements in the array equal to x. Equivalent to arrayCount (elem -\> elem = x, arr).

`NULL` elements are handled as separate values.

**Syntax**

```sql
countEqual(arr, x)
```

**Arguments**

- `arr` — Array.
- `x` - pivot element

**Returned value**

- Number of elements in the array equal to x.

**Examples**

```sql
SELECT countEqual([1, 2, NULL, NULL], NULL)
```

```plain%20text
┌─countEqual([1, 2, NULL, NULL], NULL)─┐
│ 2                                    │
└──────────────────────────────────────┘
```

## flatten

Converts an array of arrays to a flat array.Function:- Applies to any depth of nested arrays. - Does not change arrays that are already flat. The flattened array contains all the elements from all source arrays.

**Syntax**

```sql
flatten(array_of_arrays)
```

Alias: `flatten` .

**Arguments**

- `array_of_arrays` — Array of arrays. For example, `[[1,2,3], [4,5]]` .

**Returned value**

- The flattened array.

**Examples**

```sql
SELECT flatten([[[1]], [[2], [3]]]);
```

```plain%20text
┌─flatten(array(array([1]), array([2], [3])))─┐
│ [1, 2, 3]                                   │
└─────────────────────────────────────────────┘
```

## groupArrayInsertAt

Inserts a value into the array at the specified position.

**Syntax**

```sql
groupArrayInsertAt(default_x, size)(x, pos)
```

If in one query several values are inserted into the same position, the function behaves in the following ways:

- If a query is executed in a single thread, the first one of the inserted values is used.
- If a query is executed in multiple threads, the resulting value is an undetermined one of the inserted values.

**Arguments**

- `x` — Value to be inserted. Expression resulting in one of the supported data types.
- `pos` — Position at which the specified element `x` is to be inserted. Index numbering in the array starts from zero. UInt32.
- `default_x` — Default value for substituting in empty positions. Optional parameter. Expression resulting in the data type configured for the `x` parameter. If `default_x` is not defined, the default values are used.
- `size` — Length of the resulting array. Optional parameter. When using this parameter, the default value `default_x` must be specified. UInt32.

**Returned value**

- Array with inserted values.

**Example**

```sql
SELECT groupArrayInsertAt(toString(number), number * 2) FROM numbers(5);
```

```text
┌─groupArrayInsertAt(toString(number), multiply(number, 2))─┐│ ['0','','1','','2','','3','','4']                         │└───────────────────────────────────────────────────────────┘
```

```sql
SELECT groupArrayInsertAt('-')(toString(number), number * 2) FROM numbers(5);
```

```text
┌─groupArrayInsertAt('-')(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2','-','3','-','4']                          │
└────────────────────────────────────────────────────────────────┘
```

```sql
SELECT groupArrayInsertAt('-', 5)(toString(number), number * 2) FROM numbers(5);
```

```text
┌─groupArrayInsertAt('-', 5)(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2']                                             │
└───────────────────────────────────────────────────────────────────┘
```

<!-- TODO: numbers_mt is not supported. This example may need to remove -->
<!-- Multi-threaded insertion of elements into one position.

``` sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

As a result of this query you get random integer in the `[0,9]` range. For example:

``` text
┌─groupArrayInsertAt(number, 0)─┐
│ [7]                           │
└───────────────────────────────┘
``` -->

## hasAll

Checks whether one array is a subset of another.

**Syntax**

```sql
hasAll(set, subset)
```

**Arguments**

- `set` – Array of any type with a set of elements.
- `subset` – Array of any type with elements that should be tested to be a subset of `set` .

**Return values**

- `1` , if `set` contains all of the elements from `subset` .
- `0` , otherwise.

**Peculiar properties**

- An empty array is a subset of any array.
- `Null` processed as a value.
- Order of values in both of arrays does not matter.

**Examples**

```sql
SELECT hasAll([], []);
```

```text
┌─hasAll(array(), array())─┐
│ 1                        │
└──────────────────────────┘
```

```sql
SELECT hasAll([1, Null], [Null]);
```

```text
┌─hasAll([1, NULL], [NULL])─┐
│ 1                         │
└───────────────────────────┘
```

```sql
SELECT hasAll([1.0, 2, 3, 4], [1, 3]);
```

```text
┌─hasAll([1., 2, 3, 4], [1, 3])─┐
│ 1                             │
└───────────────────────────────┘
```

```sql
SELECT hasAll(['a', 'b'], ['a']);
```

```text
┌─hasAll(['a', 'b'], ['a'])─┐
│ 1                         │
└───────────────────────────┘
```

```sql
SELECT hasAll([1], ['a']);
```

```text
┌─hasAll([1], ['a'])─┐
│ 0                  │
└────────────────────┘
```

```sql
SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]]);
```

```text
┌─hasAll(array([1, 2], [3, 4]), array([1, 2], [3, 5]))─┐
│ 0                                                    │
└──────────────────────────────────────────────────────┘
```

## hasAny

Checks whether two arrays have intersection by some elements.

**Syntax**

```sql
hasAny(array1, array2)
```

**Arguments**

- `array1` – Array of any type with a set of elements.
- `array2` – Array of any type with a set of elements.

**Return values**

- `1` , if `array1` and `array2` have one similar element at least.
- `0` , otherwise.

**Peculiar properties**

- `Null` processed as a value.
- Order of values in both of arrays does not matter.

**Examples**

```sql
SELECT hasAny([1], []);
```

```text
┌─hasAny([1], array())─┐
│ 0                    │
└──────────────────────┘
```

```sql
SELECT hasAny([Null], [Null, 1]);
```

```text
┌─hasAny([NULL], [NULL, 1])─┐
│ 1                         │
└───────────────────────────┘
```

```sql
SELECT hasAny([-128, 1., 512], [1]);
```

```text
┌─hasAny([-128, 1., 512], [1])─┐
│ 1                            │
└──────────────────────────────┘
```

```sql
SELECT hasAny([[1, 2], [3, 4]], ['a', 'c']);
```

```text
┌─hasAny(array([1, 2], [3, 4]), ['a', 'c'])─┐
│ 0                                         │
└───────────────────────────────────────────┘
```

```sql
SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]]);
```

```text
┌─hasAll(array([1, 2], [3, 4]), array([1, 2], [1, 2]))─┐
│ 1                                                    │
└──────────────────────────────────────────────────────┘
```

## indexOf

Returns the index of the first ‘x’ element (starting from 1) if it is in the array, or 0 if it is not.

**Syntax**

```sql
indexOf(arr, x)
```

**Arguments**

- `arr` – Array of any type with a set of elements.
- `x` – an Element.

**Return values**

- index of the first ‘x’ element (starting from 1)

**Examples**

```sql
SELECT indexOf([1, 3, NULL, NULL], NULL);
```

```plain%20text
┌─indexOf([1, 3, NULL, NULL], NULL)─┐
│ 3                                 │
└───────────────────────────────────┘
```

Elements set to `NULL` are handled as normal values.

## length

Returns the length of a arrays.

**Syntax**

```sql
length(array)
```

**Arguments**

- `array` – Array of any type with a set of elements.

**Return values**

- length of array. UInt64

**Examples**

```sql
SELECT length([1,2,3]);
```

````text
┌─length([1, 2, 3])─┐
│ 3                 │
└───────────────────┘
```Returns an array of `UInt` numbers from 0 to `end - 1` by 1 .

**Syntax**
<!-- Different with community -->
```sql
range(end)
````

**Arguments**

- `end` — The number before which the array is constructed. Required. UInt

**Returned value**

- Array of `UInt` numbers from 0 to `end - 1` by 1 .

**Implementation details**

- All arguments must be positive values.
- An exception is thrown if the query results in arrays with a total length of more than 100,000,000 elements.

**Examples**

```sql
SELECT range(5);
```

```plain%20text
┌─range(5)────────┐
│ [0, 1, 2, 3, 4] │
└─────────────────┘
```
