---
title: "Mathematical"
slug: "mathematical"
hidden: false
createdAt: "2021-07-29T12:24:09.667Z"
updatedAt: "2021-09-23T06:33:34.199Z"
tags:
  - Docs
---

> Notice:
> Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

## COVAR_POP

Calculates the value of `Σ((x - x̅)(y - y̅)) / n` .

Note: This function uses a numerically unstable algorithm. If you need numerical stability in calculations, use the `covarPopStable` function. It works slower but provides a lower computational error.

**Syntax**

```sql
covarPop(x, y)
```

**Arguments**

- `x` – The set of number.
- `y` - The set of number.

**Returned value**

- The population covariance.

Type: `Float64`

**Example**

```sql
CREATE TABLE test.test_covarPop(days_employed Int32, salary Int32) ENGINE = CnchMergeTree ORDER BY days_employed; -- create sample table
INSERT INTO test.test_covarPop(days_employed,salary) VALUES(300,3000),(600,4000),(900,4500),(1200,4800),(1500,5000); -- insert data to table
SELECT covarPop(days_employed,salary) FROM test.test_covarPop; -- find out the population covariance for days employed and salary
```

Result:

```plain%20text
┌─covarPop(days_employed, salary)─┐
│ 2.88e+05                        │
└─────────────────────────────────┘
```

## COVAR_SAMP

Calculates the value of `Σ((x - x̅)(y - y̅)) / (n - 1)` .

Note:This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the `covarSampStable` function. It works slower but provides a lower computational error.

**Syntax**

```sql
covarSamp(x, y)
```

**Arguments**

- `x` – The set of number.
- `y` - The set of number.

**Returned value**

- The sample covariance, when `n <= 1` , returns +∞.

Type: `Float64`

**Example**

```sql
CREATE TABLE test.test_covarSamp(days_employed Int32, salary Int32) ENGINE = CnchMergeTree ORDER BY days_employed; -- create sample table
INSERT INTO test.test_covarSamp(days_employed,salary) VALUES(300,3000),(600,4000),(900,4500),(1200,4800),(1500,5000); -- insert data to table
SELECT covarSamp(days_employed,salary) FROM test.test_covarSamp; -- find out the sample covariance for days employed and salary
```

Result:

```plain%20text
┌─covarSamp(days_employed, salary)─┐
│ 3.6e+05                          │
└──────────────────────────────────┘
```

## acos

The arc cosine.

**Syntax**

```sql
acos(x)
```

**Arguments**

- `x` – The radians.

**Returned value**

- Return radians.

Type: `Float64`

**Example**

```sql
SELECT acos(-1);
```

Result:

```plain%20text
┌─ACOS(-1)──────────────┐
│ 3.141592653589793e+00 │
└───────────────────────┘
```

## asin

The arc sine.

**Syntax**

```sql
asin(x)
```

**Arguments**

- `x` – The radians.

**Returned value**

- Return radians.

Type: `Float64`

**Example**

```sql
SELECT asin(-1);
```

Result:

```plain%20text
┌─asin(1)────────────────┐
│ 1.5707963267948966e+00 │
└────────────────────────┘
```

## atan

The arc tangent.

**Syntax**

```sql
atan(x)
```

**Arguments**

- `x` – The radians.

**Returned value**

- Return radians.

Type: `Float64`

**Example**

```sql
SELECT atan(-1);
```

Result:

```plain%20text
┌─atan(-1)───────────────┐
│ -7.853981633974483e-01 │
└────────────────────────┘
```

## cbrt

Accepts a numeric argument and returns a Float64 number close to the cubic root of the argument.

**Syntax**

```sql
cbrt(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The root of the argument.

Type:`Float64`

**Example**

```sql
SELECT cbrt(8)
```

Result:

```plain%20text
┌─cbrt(8)─┐
│ 2e+00   │
└─────────┘
```

## ceil

Returns the smallest round number that is greater than or equal to `x` . In every other way, it is the same as the `floor` function (see above).

**Syntax**

```sql
ceil(x[, N]), ceiling(x[, N])
```

**Arguments**

- `x` – The number.
- `N` — `decimal-places`, An integer value.

**Returned value**

- The round number.

Type: `Float64`

**Example**

```sql
SELECT ceil(1.99,2);
```

Result:

```plain%20text
┌─ceil(1.99, 2)─┐
│ 1.99e+00      │
└───────────────┘
```

other example:

```sql
SELECT ceil(1.99,1);
```

Result:

```plain%20text
┌─ceil(1.99, 1)─┐
│ 2e+00         │
└───────────────┘
```

## ceiling

Returns the smallest round number that is greater than or equal to `x` . In every other way, it is the same as the `floor` function (see above).

**Syntax**

```sql
ceil(x[, N]), ceiling(x[, N])
```

**Arguments**

- `x` – The number.
- `N` - The integer of rounding decimal place.

**Returned value**

- The round number.

Type: `Float64`

**Example**

```sql
SELECT ceiling(1.99,2);
```

Result:

```plain%20text
┌─ceil(1.99, 2)─┐
│ 1.99e+00      │
└───────────────┘
```

other example:

```sql
SELECT ceiling(1.99,1);
```

Result:

```plain%20text
┌─ceil(1.99, 1)─┐
│ 2e+00         │
└───────────────┘
```

## cos

The cosine.

**Syntax**

```sql
cos(x)
```

**Arguments**

- `x` – The radians.

**Returned value**

- Return radians.

Type: `Float64`

**Example**

```sql
SELECT cos(pi())
```

Result:

```plain%20text
┌─cos(pi())─┐
│ -1e+00    │
└───────────┘
```

## erf

The error function `erf(x)=2√π∫x0e−t2dt erf(x)` .

Note: If ‘x’ is non-negative, then `erf(x / σ√2)` is the probability that a random variable having a normal distribution with standard deviation ‘σ’ takes the value that is separated from the expected value by more than ‘x’.

**Syntax**

```sql
erf(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The probability.

Type: `Float64`

**Example**

```sql
SELECT erf(3 / sqrt(2));
```

```plain%20text
┌─erf(divide(3, sqrt(2)))─┐
│ 9.973002039367398e-01   │
└─────────────────────────┘
```

Note: three sigma rule

## erfc

The complementary error function follows the formula: erfc(x) = 1 − erf(x).

Accepts a numeric argument and returns a Float64 number close to 1 - erf(x), but without loss of precision for large `x` values.

**Syntax**

```sql
erfc(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The probability.

Type: `Float64`

**Example**

```sql
SELECT erfc(3 / sqrt(2));
```

```plain%20text
┌─erfc(divide(3, sqrt(2)))─┐
│ 2.6997960632601913e-03   │
└──────────────────────────┘
```

Note: three sigma rule

## exp

Accepts a numeric argument and returns a Float64 number close to the exponent of the argument.

**Syntax**

```sql
exp(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of calculation.

Type: `Float64`

**Example**

```sql
SELECT exp(1);
```

Result:

```plain%20text
┌─exp(1)────────────────┐
│ 2.718281828459045e+00 │
└───────────────────────┘
```

## exp10

Accepts a numeric argument and returns a Float64 number close to 10 to the power of `x`.

**Syntax**

```sql
exp10(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of calculation.

Type: `Float64`

**Example**

```sql
SELECT exp10(3);
```

Result:

```plain%20text
┌─exp10(3)─┐
│ 1e+03    │
└──────────┘
```

## exp2

Accepts a numeric argument and returns a Float64 number close to 2 to the power of `x`.

**Syntax**

```sql
exp2(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of calculation.

calculating
Type: `Float64`

**Example**

```sql
SELECT exp2(3);
```

Result:

```plain%20text
┌─exp2(3)─┐
│ 8e+00   │
└─────────┘
```

## floor

Returns the largest round number that is less than or equal to `x` . A round number is a multiple of 1/10N, or the nearest number of the appropriate data type if 1 / 10N isn’t exact.

- `N` is an integer constant, optional parameter. By default it is zero, which means to round to an integer.

- `N` may be negative.

- `x` is any numeric type. The result is a number of the same type.

For integer arguments, it makes sense to round with a negative `N` value (for non-negative `N` , the function does not do anything).

If rounding causes overflow (for example, floor(-128, -1)), an implementation-specific result is returned.

**Syntax**

```sql
floor(x[, N])
```

**Arguments**

- `x` – The number.

- `N` – round to an integer

**Returned value**

- The result of calculation.

Type: `Float64`

**Example**

```sql
SELECT floor(123.45, 1);
```

Result:

```plain%20text
┌─floor(123.45, 1)─┐
│ 1.234e+02        │
└──────────────────┘
```

other example:

```sql
select floor(123.45, -1);
```

Result:

```plain%20text
┌─floor(123.45, -1)─┐
│ 1.2e+02           │
└───────────────────┘
```

## intExp10

Accepts a numeric argument and returns a UInt64 number close to 10 to the power of `x`.

**Syntax**

```sql
intExp10(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of calculation.

Type: `UInt64`

**Example**

```sql
SELECT intExp10(3);
```

Result:

```plain%20text
┌─intExp10(3)─┐
│ 1000        │
└─────────────┘
```

## intExp2

Accepts a numeric argument and returns a UInt64 number close to 2 to the power of `x`.

**Syntax**

```sql
intExp2(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of calculation.

Type: `UInt64`

**Example**

```sql
SELECT intExp2(3);
```

Result:

```plain%20text
┌─intExp2(3)─┐
│ 8          │
└────────────┘
```

## lgamma

The logarithm of the gamma function.

**Syntax**

```sql
lgamma(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of calculation.

Type: `Float64`

**Example**

```sql
SELECT lgamma(3);
```

Result:

```plain%20text
┌─lgamma(3)─────────────┐
│ 6.931471805599453e-01 │
└───────────────────────┘
```

## ln

Accepts a numeric argument and returns a Float64 number close to the natural logarithm of the argument.

**Syntax**

```sql
ln(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of calculation.

Type: `Float64`

**Example**

```sql
select ln(2.7182818)
```

Result:

```plain%20text
┌─ln(2.7182818)─────────┐
│ 9.999999895305024e-01 │
└───────────────────────┘
```

## log10

Accepts a numeric argument and returns a Float64 number close to the decimal logarithm of the argument.

**Syntax**

```sql
log10(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of calculation.

Type: `Float64`

**Example**

```sql
SELECT log10(3);
```

Result:

```plain%20text
┌─log10(3)───────────────┐
│ 4.7712125471966244e-01 │
└────────────────────────┘
```

## log2

Accepts a numeric argument and returns a Float64 number close to the binary logarithm of the argument.

**Syntax**

```sql
log2(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of calculation.

Type: `Float64`

**Example**

```sql
select log2(3);
```

Result:

```plain%20text
┌─log2(3)───────────────┐
│ 1.584962500721156e+00 │
└───────────────────────┘
```

## pi

Returns a Float64 number that is close to the number π.

**Syntax**

```sql
pi()
```

**Arguments**

- N.A

**Returned value**

- The value of π.

Type: `Float64`

**Example**

```sql
SELECT pi();
```

Result:

```plain%20text
┌─pi()──────────────────┐
│ 3.141592653589793e+00 │
└───────────────────────┘
```

## pow

Takes two numeric arguments `x` and `y`. Returns a Float64 number close to `x` to the power of `y`.

**Syntax**

```sql
pow(x, y)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of calculation.

Type: `Float64`

**Example**

```sql
SELECT pow(2, 3);
```

Result:

```plain%20text
┌─pow(2, 3)─┐
│ 8e+00     │
└───────────┘
```

## power

Takes two numeric arguments `x` and `y`. Returns a Float64 number close to `x` to the power of `y`.

Alias：pow

## round

Rounds a value to a specified number of decimal places.

The function returns the nearest number of the specified order. In case when given number has equal distance to surrounding numbers, the function uses banker’s rounding for float number types and rounds away from zero for the other number types.

**Syntax**

```sql
round(expression [, decimal_places])
```

**Arguments**

- `expression` — A number to be rounded. Can be any expression returning the numeric data type .

- `decimal-places` — An integer value.

  - If `decimal-places > 0` then the function rounds the value to the right of the decimal point.

  - If `decimal-places < 0` then the function rounds the value to the left of the decimal point.

  - If `decimal-places = 0` then the function rounds the value to integer. In this case the argument can be omitted.

**Returned value:**

- The rounded number of the same type as the input number.

Type: `Float64`

**Example**

```sql
SELECT round(1.1234);
```

Result:

```plain%20text
┌─round(1.1234)─┐
│ 1e+00         │
└───────────────┘
```

other example

```sql
SELECT round(1.1234,2);
```

Result:

```plain%20text
┌─round(1.1234, 2)─┐
│ 1.12e+00         │
└──────────────────┘
```

## roundAge

Accepts a number. If the number is less than 17, it returns 17. Otherwise, it rounds the number down to a number from the set: 17, 25, 35, 45, 55. This function is specific to Yandex.Metrica and used for implementing the report on user age.

**Syntax**

```sql
roundAge(num)
```

**Arguments**

- `num` – The age.

**Returned value**

- A rounded value..

Type: `UInt8`

**Example**

```sql
SELECT roundAge(50);
```

Result:

```plain%20text
┌─roundAge(50)─┐
│ 45           │
└──────────────┘
```

other example

```sql
SELECT roundAge(16), roundAge(17),roundAge(18);
```

Result:

```plain%20text
┌─roundAge(16)─┬─roundAge(17)─┬─roundAge(18)─┐
│ 17           │ 17           │ 18           │
└──────────────┴──────────────┴──────────────┘
```

## roundDown

Accepts a number and rounds it down to an element in the specified array. If the value is less or greater than the bound, the lowest or greatest bound is returned.

**Syntax**

```sql
roundDown(number, array)
```

**Arguments**

- `number` – The number.
- `array` – The array.

**Returned value**

- The result of roundng.

**Example**

```sql
SELECT roundDown(2, [6, 7, 8]);
```

Result:

```plain%20text
┌─roundDown(2, [6, 7, 8])─┐
│ 6                       │
└─────────────────────────┘
```

## roundDuration

Accepts a `number`. If the number is less than one, it returns 0. Otherwise, it rounds the number down to numbers from the set: 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000. This function is specific to Yandex. Metrica and used for implementing the report on session length.

**Syntax**

```sql
roundDuration(number)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of rounding.

**Example**

```sql
SELECT roundDuration(230);
```

Result:

```plain%20text
┌─roundDuration(230)─┐
│ 180                │
└────────────────────┘
```

## roundToExp2

Accepts a `number`. If the `number` is less than one, it returns 0. Otherwise, it rounds the `number` down to the nearest (whole non-negative) degree of two.

**Syntax**

```sql
roundToExp2(number)
```

**Arguments**

- `number` – The number.

**Returned value**

- The result of rounding.

**Example**

```sql
SELECT roundToExp2(31);
```

Result:

```plain%20text
┌─roundToExp2(31)─┐
│ 16              │
└─────────────────┘
```

## sin

The sine.

**Syntax**

```sql
sin(x)
```

**Arguments**

- `x` – The radians.

**Returned value**

- Return radians.

Type: `Float64`

**Example**

```sql
SELECT sin(pi()/2)
```

Result:

```plain%20text
┌─sin(divide(pi(), 2))─┐
│ 1e+00                │
└──────────────────────┘
```

## sqrt

Accepts a numeric argument and returns a Float64 number close to the square root of the argument.

**Syntax**

```sql
sqrt(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The square root.

Type: `Float64`

**Example**

```sql
SELECT sqrt(4);
```

Result:

```plain%20text
┌─sqrt(4)─┐
│ 2e+00   │
└─────────┘
```

## tan

The tangent.

**Syntax**

```sql
tan(x)
```

**Arguments**

- `x` – The radians.

**Returned value**

- Return radians.

Type: `Float64`

**Example**

```sql
SELECT tan(pi()/4);
```

Result:

```plain%20text
┌─tan(divide(pi(), 4))──┐
│ 9.999999999999999e-01 │
└───────────────────────┘
```

Note: the result has particular precision, it probably will be fixed with next minor release.## tgamma
Computes the gamma function of arg.

**Syntax**

```sql
tgamma(x)
```

**Arguments**

- `x` – The number.

**Returned value**

- The result of calculation.

**Example**

```sql
select tgamma(10);
```

Result:

```plain%20text
┌─tgamma(10)─────────────┐
│ 3.6287999999999994e+05 │
└────────────────────────┘
```

## trunc

Returns the round number with largest absolute value that has an absolute value less than or equal to `x` ‘s. In every other way, it is the same as the ’floor’ function (see above).

**Syntax**

```sql
trunc(x[, N])
truncate(x[, N])
```

**Arguments**

- `x` – The number.
- `N` - The integer of rounding decimal place.

**Returned value**

- The rouded number.

**Example**

```sql
SELECT trunc(100.11, 1)
```

Result:

```plain%20text
┌─trunc(100.11, 1)─┐
│ 1.001e+02        │
└──────────────────┘
```

## truncate

Removes all data from a table. When the clause `IF EXISTS` is omitted, the query returns an error if the table does not exist.

The `TRUNCATE` query is not supported for View, File, URL, Buffer and Null table engines.

**Syntax**

```sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

**Arguments**

- `name` – The table name.
- `[IF EXISTS]` - Optional, the query returns an error if the table does not exist.
- `[db.]` - Optional, the database name.
- `[ON CLUSTER cluster]` - Optional, the cluster name.

**Returned value**

- N.A.

**Example**

```sql
CREATE TABLE test.test_truncate (id Int32) ENGINE = CnchMergeTree ORDER BY id;
INSERT INTO test.test_truncate(id) VALUES(1),(2),(3),(4),(5); -- insert 1,2,3,4,5 to table
SELECT * FROM test.test_truncate; -- check the date before truncate
```

```plain%20text
┌─id─┐
│ 1  │
│ 2  │
│ 3  │
│ 4  │
│ 5  │
└────┘
```

```sql
TRUNCATE TABLE test.`test_truncate`

SELECT * FROM test.`test_truncate`
```

Result:

```plain%20text

```
