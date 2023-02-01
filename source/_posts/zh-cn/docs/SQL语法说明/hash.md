---
title: "Hash"
slug: "hash"
hidden: false
createdAt: "2021-07-29T12:05:52.839Z"
updatedAt: "2021-09-23T04:02:59.496Z"
categories:
- Docs
- SQL_Syntax
tags:
- Docs
---

# hash

> Notice:
> Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

## MD5

Calculates the MD5 from a string and returns the resulting set of bytes as FixedString(16).

If you do not need MD5 in particular, but you need a decent cryptographic 128-bit hash, use the [sipHash128] function instead.

If you want to get the same result as output by the md5sum utility, use lower(hex(MD5(s))).

**Syntax**

```
select MD5(string)

```

**Arguments**

- `string` – A string

**Returned value**

- The Value of MD5.

Type: FixedString(16)

**Example**

```
select MD5('test the MD5 function');

```

Result:

```
┌─MD5('test the MD5 function')─┐
│ ोNiq@5G:a               │
└──────────────────────────────┘

```

## SHA1

Calculates SHA-1, SHA-224, or SHA-256 from a string and returns the resulting set of bytes as FixedString(20), FixedString(28), or FixedString(32).

The function works fairly slowly (SHA-1 processes about 5 million short strings per second per processor core, while SHA-224 and SHA-256 process about 2.2 million).

We recommend using this function only in cases when you need a specific hash function and you can’t select it.

Even in these cases, we recommend applying the function offline and pre-calculating values when inserting them into the table, instead of applying it in SELECTS.

**Syntax**

```
SHA1(string)

```

**Arguments**

- `string` – A string.

**Returned value**

- SHA-1 Encrypted string in `FixedString(20)`.

Type: `FixedString(20)`

**Example**

```
SELECT base64Encode(toString(SHA1('SAH1 test')));

```

Result:

```
┌─base64Encode(toString(SHA1('SAH1 test')))─┐
│ x5bDmJsgE+YzTir1+BPt4S98AEc=              │
└───────────────────────────────────────────┘

```

## SHA224

Calculates SHA-1, SHA-224, or SHA-256 from a string and returns the resulting set of bytes as FixedString(20), FixedString(28), or FixedString(32).

The function works fairly slowly (SHA-1 processes about 5 million short strings per second per processor core, while SHA-224 and SHA-256 process about 2.2 million).

We recommend using this function only in cases when you need a specific hash function and you can’t select it.

Even in these cases, we recommend applying the function offline and pre-calculating values when inserting them into the table, instead of applying it in SELECTS.

**Syntax**

```
SHA224(string)

```

**Arguments**

- `string` – A string.

**Returned value**

- SHA-224 Encrypted string in `FixedString(28)`.

Type: `FixedString(28)`

**Example**

```
SELECT base64Encode(toString(SHA224('SAH224 test')));

```

Result:

```
┌─base64Encode(toString(SHA224('SAH224 test')))─┐
│ eptUvYjJG4AeQfQI9kZ/qViECg0gRbuwPJ5UlA==      │
└───────────────────────────────────────────────┘

```

## SHA256

Calculates SHA-1, SHA-224, or SHA-256 from a string and returns the resulting set of bytes as FixedString(20), FixedString(28), or FixedString(32).

The function works fairly slowly (SHA-1 processes about 5 million short strings per second per processor core, while SHA-224 and SHA-256 process about 2.2 million).

We recommend using this function only in cases when you need a specific hash function and you can’t select it.

Even in these cases, we recommend applying the function offline and pre-calculating values when inserting them into the table, instead of applying it in SELECTS.

**Syntax**

```
SHA256(string)

```

**Arguments**

- `string` – A string.

**Returned value**

- SHA-256 Encrypted string in `FixedString(32)`.

Type: `FixedString(32)`

**Example**

```
SELECT base64Encode(toString(SHA256('SAH256 test')));

```

Result:

```
┌─base64Encode(toString(SHA256('SAH256 test')))─┐
│ I+OvFrLmD2Ofq1xBuFmKYCuB6iSg5/OrhcIbI5Qezs8=  │
└───────────────────────────────────────────────┘

```

## URLHash

A fast, decent-quality non-cryptographic hash function for a string obtained from a URL using some type of normalization.

`URLHash(s)` – Calculates a hash from a string without one of the trailing symbols `/` , `?` or `#` at the end, if present.

`URLHash(s, N)` – Calculates a hash from a string up to the N level in the URL hierarchy, without one of the trailing symbols `/` , `?` or `#` at the end, if present.

Levels are the same as in URLHierarchy. This function is specific to Yandex.Metrica.

**Syntax**

```
URLHash(s,N)

```

**Arguments**

- `s` – The URL string.
- `N` – The number of level in UInt.

**Returned value**

- The hash value in UInt64

Type: `Uint64`

**Example**

```
SELECT URLHash('https://www.bytedance.com/en/news',2);

```

Result:

```
┌─URLHash('https://www.bytedance.com/en/news', 2)─┐
│ 11898456355197509749                            │
└─────────────────────────────────────────────────┘

```

## cityHash64

Produces a 64-bit [CityHash](https://github.com/google/cityhash) hash value.

This is a fast non-cryptographic hash function. It uses the CityHash algorithm for string parameters and implementation-specific fast non-cryptographic hash function for parameters with other data types. The function uses the CityHash combinator to get the final results.

**Syntax**

```
cityHash64(par1,...)

```

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types](https://bytedance.feishu.cn/sql-reference/data-types/index.md) .

**Returned Value**

A `UInt64` data type hash value.

Type:`UInt64`

**Examples**

Call example:

```
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type;

```

Result:

```
┌─CityHash─────────────┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘

```

The following example shows how to compute the checksum of the entire table with accuracy up to the row order:

```
SELECT groupBitXor(cityHash64(*)) FROM table;

```

## farmHash64

Produces a 64-bit [FarmHash] or Fingerprint value. `farmFingerprint64` is preferred for a stable and portable value.

**Syntax**

```
farmFingerprint64(par1, ...)

farmHash64(par1, ...)

```

These functions use the `Fingerprint64` and `Hash64` methods respectively from all [available methods] .

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types].

**Returned Value**

An `UInt64` data type hash value.

Tyep:`UInt64`

**Example**

```
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type;

```

Result:

```
┌─FarmHash─────────────┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘

```

## gccMurmurHash

Calculates a 64-bit [MurmurHash2](https://github.com/aappleby/smhasher) hash value using the same hash seed as [gcc](https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc++-v3/include/bits/functional_hash.h#L191) . It is portable between CLang and GCC builds.

**Syntax**

```
gccMurmurHash(par1, ...)

```

**Arguments**

- `par1, ...` — A variable number of parameters that can be any of the [supported data types].

**Returned value**

- Calculated hash value.

Type: `UInt64`.

**Example**

Query:

```
SELECT gccMurmurHash(1, 2, 3) AS res1,gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) AS res2;

```

Result:

```
┌─res1─────────────────┬─res2────────────────┐
│ 12384823029245979431 │ 1188926775431157506 │
└──────────────────────┴─────────────────────┘

```

## halfMD5

[Interprets] all the input parameters as strings and calculates the [MD5](https://en.wikipedia.org/wiki/MD5) hash value for each of them. Then combines hashes, takes the first 8 bytes of the hash of the resulting string, and interprets them as `UInt64` in big-endian byte order.

**Syntax**

```
halfMD5(par1, ...)

```

The function is relatively slow (5 million short strings per second per processor core).

Consider using the [sipHash64] function instead.

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types].

**Returned Value**

An [UInt64] data type hash value.

Type: `Uini64`

**Example**

```
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type;

```

```
┌─halfMD5hash────────┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘

```

## hiveHash

Calculates `HiveHash` from a string.

**Syntax**

```
SELECT hiveHash('')

```

This is just [JavaHash] with zeroed out sign bit. This function is used in [Apache Hive](https://en.wikipedia.org/wiki/Apache_Hive) for versions before 3.0. This hash function is neither fast nor having a good quality. The only reason to use it is when this algorithm is already used in another system and you have to calculate exactly the same result.

**Returned value**

A `Int32` data type hash value.

Type: `hiveHash` .

**Example**

Query:

```
SELECT hiveHash('Hello, world!');

```

Result:

```
┌─hiveHash('Hello, world!')─┐
│ 267439093                 │
└───────────────────────────┘

```

## intHash32

Calculates a 32-bit hash code from any type of integer.

This is a relatively fast non-cryptographic hash function of average quality for numbers.

TODO: need to re-confirm below sentences.

**Syntax**

```
intHash32(integer)

```

**Arguments**

- `integer` – The integer.

**Returned value**

- 32-bit hash code.

Type:`UInt32`

**Example**

```
SELECT intHash32(12072650598913549138);

```

Result:

```
┌─intHash32(12072650598913549138)─┐
│ 3406876673                      │
└─────────────────────────────────┘

```

## intHash64

Calculates a 64-bit hash code from any type of integer.

It works faster than intHash32. Average quality.

TODO: need to re-confirm below sentences.

**Syntax**

```
intHash64(integer)

```

**Arguments**

- `integer` – The integer.

**Returned value**

- 64-bit hash code.

Type:`UInt64`

**Example**

```
SELECT intHash64(12072650598913549138);

```

Result:

```
┌─intHash32(12072650598913549138)─┐
│ 17337913903528382330            │
└─────────────────────────────────┘

```

## javaHash

Calculates [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) from a string. This hash function is neither fast nor having a good quality. The only reason to use it is when this algorithm is already used in another system and you have to calculate exactly the same result.

**Syntax**

```
SELECT javaHash('')

```

**Arguments**

- A `string`

**Returned value**

- A `Int32` data type hash value.

**Example**

Query:

```
SELECT javaHash('Hello, world!');

```

Result:

```
┌─javaHash('Hello, world!')─┐
│ -1880044555               │
└───────────────────────────┘

```

## jumpConsistentHash

Calculates JumpConsistentHash form a UInt64.

Accepts two arguments: a `UInt64`-type key and the number of buckets. Returns `Int32`.

For more information, see the link: [JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

**Syntax**

```
JumpConsistentHash(key, buckets)

```

**Arguments**

- `key` – An `UInt64`- type key.
- `buckets` – A number of buckets.

**Returned value**

- Result in `Int32`.

Type：`Int32`

**Example**

TODO: improve below example if necessary

```
SELECT jumpConsistentHash(18446744073709551615, 12);

```

Result:

```
┌─jumpConsistentHash(18446744073709551615, 12)─┐
│ 10                                           │
└──────────────────────────────────────────────┘

```

## metroHash64

Produces a 64-bit [MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/) hash value.

**Syntax**

```
metroHash64(par1, ...)

```

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types] .

**Returned Value**

A [UInt64] data type hash value.

Type：`UInt64`

**Example**

```
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type;

```

```
┌─MetroHash────────────┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘

```

## murmurHash2_32

Produces a [MurmurHash2](https://github.com/aappleby/smhasher/) hash value.

**Syntax**

```
murmurHash2_32(par1, ...)

```

**Arguments**

Both functions take a variable number of input parameters. Arguments can be any of the [supported data types] .

**Returned Value**

- The `murmurHash2_32` function returns hash value having the [UInt32] data type.

Type: `Uint32`

**Example**

```
SELECT murmurHash2_32('test');

```

Result:

```
┌─murmurHash2_32('test')─┐
│ 403862830              │
└────────────────────────┘

```

## murmurHash2_64

Produces a [MurmurHash2](https://github.com/aappleby/smhasher) hash value.

**Syntax**

```
murmurHash2_64(par1, ...)

```

**Arguments**

Both functions take a variable number of input parameters. Arguments can be any of the supported data types .

**Returned Value**

- The `murmurHash2_64` function returns hash value having the UInt64 data type.

Type: `Uint64`

**Example**

```
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type;

```

```
┌─MurmurHash2──────────┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘

```

## murmurHash3_128

Produces a 128-bit [MurmurHash3](https://github.com/aappleby/smhasher) hash value.

**Syntax**

```
murmurHash3_128( expr )

```

**Arguments**

- `expr` — Expressions returning a String -type value.

**Returned Value**

- A FixedString(16) data type hash value.

Type: `FixedString(16)`

**Example**

```
SELECT hex(murmurHash3_128('example_string')) AS MurmurHash3, toTypeName(MurmurHash3) AS type;

```

Result:

```
┌─MurmurHash3──────────────────────┬─type───┐
│ 368A1A311CB7342253354B548E7E7E71 │ String │
└──────────────────────────────────┴────────┘

```

## murmurHash3_32

Produces a [MurmurHash3](https://github.com/aappleby/smhasher) hash value.

**Syntax**

```
murmurHash3_32(par1, ...)

```

**Arguments**

Both functions take a variable number of input parameters. Arguments can be any of the supported data types .

**Returned Value**

- The `murmurHash3_32` function returns a UInt32 data type hash value.

Type: `Uint32`

**Example**

```
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type;

```

Result:

```
┌─MurmurHash3─┬─type───┐
│ 2152717     │ UInt32 │
└─────────────┴────────┘

```

## murmurHash3_64

Produces a [MurmurHash3](https://github.com/aappleby/smhasher) hash value.

**Syntax**

```
murmurHash3_64(par1, ...)

```

**Arguments**

Both functions take a variable number of input parameters. Arguments can be any of the supported data types .

**Returned Value**

- The `murmurHash3_64` function returns a UInt64 data type hash value.

Type：`Uint64`

**Example**

```
SELECT murmurHash3_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type;

```

Result:

```
┌─MurmurHash3──────────┬─type───┐
│ 12344512753450997224 │ UInt64 │
└──────────────────────┴────────┘

```

## sipHash128

Calculates SipHash from a string.

Accepts a String-type argument. Returns FixedString(16).

Differs from sipHash64 in that the final xor-folding state is only done up to 128 bits.

**Syntax**

```
sipHash128(par1,...)

```

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the supported data types .

**Returned Value**

A UInt64 data type hash value.

Type: `Uint64`

**Example**

```
select sipHash128('test sipHash128');

```

Result:

```
┌─sipHash128('test sipHash128')─┐
│ av'"?k:                       │
└───────────────────────────────┘

```

## sipHash64

Produces a 64-bit [SipHash](https://github.com/veorq/SipHash/) hash value.

This is a cryptographic hash function. It works at least three times faster than the MD5 function.

Function interprets all the input parameters as strings and calculates the hash value for each of them. Then combines hashes by the following algorithm:

- After hashing all the input parameters, the function gets the array of hashes.
- Function takes the first and the second elements and calculates a hash for the array of them.
- Then the function takes the hash value, calculated at the previous step, and the third element of the initial hash array, and calculates a hash for the array of them.
- The previous step is repeated for all the remaining elements of the initial hash array.

**Syntax**

```
sipHash64(par1,...)

```

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the supported data types .

**Returned Value**

A UInt64 data type hash value.

Type: `Uint64`

**Example**

```
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type;

```

Result:

```
┌─SipHash──────────────┬─type───┐
│ 13726873534472839665 │ UInt64 │
└──────────────────────┴────────┘

```

## xxHash32

Calculates `xxHash` from a string.

`xxHash` is an extremely fast non-cryptographic hash algorithm, working at RAM speed limit.

**Syntax**

```
SELECT xxHash32(s)

```

**Arguments**

- `s` – The string.

**Returned value**

A `Uint32` data type hash value.

Type: `UInt32`

**Example**

```
SELECT xxHash32('Hello, world!');

```

Result:

```
┌─xxHash32('Hello, world!')─┐
│ 834093149                 │
└───────────────────────────┘

```

**See Also**

- [xxHash](http://cyan4973.github.io/xxHash/) .

## xxHash64

Calculates `xxHash` from a string.

`xxHash` is an extremely fast non-cryptographic hash algorithm, working at RAM speed limit.

**Syntax**

```
SELECT xxHash64(s)

```

**Arguments**

- `s` – The string.

**Returned value**

A `Uint64` data type hash value.

Type: `UInt64`

**Example**

```
SELECT xxHash64('Hello, world!');

```

Result:

```
┌─xxHash64('Hello, world!')─┐
│ 17691043854468224118      │
└───────────────────────────┘

```

**See Also**

- [xxHash](http://cyan4973.github.io/xxHash/) .
