---
title: "String"
slug: "string"
hidden: false
createdAt: "2021-07-29T12:26:32.479Z"
updatedAt: "2021-09-23T06:38:57.511Z"
categories:
- Docs
- SQL_Syntax
tags:
- Docs
---

# string

> Notice:
> Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

## CHARACTER_LENGTH

Returns the length of a string in Unicode code points (not in characters), assuming that the `string` contains a set of bytes that make up UTF-8 encoded text. If this assumption is not met, it returns some result (it does not throw an exception).

**Syntax**

```
CHARACTER_LENGTH(string)

```

**Arguments**

- `string` – The String.

**Returned value**

- The length of Char.

Type: `UInt64`

**Example**

```
select CHARACTER_LENGTH('abcdef123');

```

Result:

```
┌─CHARACTER_LENGTH('abcdef123')─┐
│ 9                             │
└───────────────────────────────┘

```

TODO：is this one same as CHAR_LENGTH ?

## CHAR_LENGTH

Returns the length of a string in Unicode code points (not in characters), assuming that the `string` contains a set of bytes that make up UTF-8 encoded text. If this assumption is not met, it returns some result (it does not throw an exception).

**Syntax**

```
CHAR_LENGTH(s)

```

**Arguments**

- `s` – The string.

**Returned value**

- The length of char.

Type: `UInt64`

**Example**

```
SELECT CHAR_LENGTH('abcdef123');

```

Result:

```
┌─CHAR_LENGTH('abcdef123')─┐
│ 9                        │
└──────────────────────────┘

```

## alphaTokens

Selects substrings of consecutive bytes from the ranges a-z and A-Z.Returns an array of substrings.

**Syntax**

```
alphaTokens(string)

```

**Arguments**

- `string` – The string.

**Returned value**

- The array of substrings.

**Example**

```
SELECT alphaTokens('abca1abc');

```

```
┌─alphaTokens('abca1abc')─┐
│ [abca, abc]             │
└─────────────────────────┘

```

## appendTrailingCharIfAbsent

If the `string` is non-empty and does not contain the `character` at the end, it appends the `character` to the end.

**Syntax**

```
appendTrailingCharIfAbsent(string, character)

```

**Arguments**

- `string` – The string.
- `character` – the character.

**Returned value**

- The character appended to string.

Type: `string`

**Example**

```
SELECT appendTrailingCharIfAbsent('abc', '2');

```

Result:

```
┌─appendTrailingCharIfAbsent('abc', '2')─┐
│ abc2                                   │
└────────────────────────────────────────┘

```

## arrayStringConcat

Concatenates the strings listed in the array with the separator.`separator` is an optional parameter: a constant string, set to an empty string by default.

**Syntax**

```
arrayStringConcat(array[, separator])

```

**Arguments**

- `array` – The array.
- `separator` – optional, separator.

**Returned value**

- The concated string.

Type: `string`

**Example**

```
SELECT arrayStringConcat(['abc','123']);

```

Result:

```
┌─arrayStringConcat(['abc', '123'])─┐
│ abc123                            │
└───────────────────────────────────┘

```

other example

```
SELECT arrayStringConcat(['abc', '123'], ',');

```

Result:

```
┌─arrayStringConcat(['abc', '123'], ',')─┐
│ abc,123                                │
└────────────────────────────────────────┘

```

## base64Decode

Decode base64-encoded `string` into original string. In case of failure raises an exception.

Alias: `FROM_BASE64` .

**Syntax**

```
base64Decode(string)

```

**Arguments**

- `string` – The string.

**Returned value**

- The decoded string.
-

**Example**

```
SELECT base64Decode('SGVsbG8gQmFzZTY0');

```

Result:

```
┌─base64Decode('SGVsbG8gQmFzZTY0')─┐
│ Hello Base64                     │
└──────────────────────────────────┘

```

## base64Encode

Encodes `string` into base64

Alias: `TO_BASE64` .

**Syntax**

```
base64Encode(string)

```

**Arguments**

- `string` – The string.

**Returned value**

- The encoded string.
-

**Example**

```
SELECT base64Encode('Hello Base64');

```

Result:

```
┌─base64Encode('Hello Base64')─┐
│ SGVsbG8gQmFzZTY0             │
└──────────────────────────────┘

```

## concat

Concatenates the strings listed in the arguments, without a separator.

**Syntax**

```
concat(s1, s2, ...)

```

**Arguments**

- `s1`, `s2` ... – Values of type String or FixedString.

**Returned values**

- The string that results from concatenating the arguments.

If any of argument values is `NULL` , `concat` returns `NULL` .

**Example**

```
SELECT concat('Hello, ', 'World!');

```

Result:

```
┌─concat('Hello, ', 'World!')─┐
│ Hello, World!               │
└─────────────────────────────┘

```

## concatAssumeInjective

Same as concat , the difference is that you need to ensure that `concat(s1, s2, ...) → sn` is injective, it will be used for optimization of GROUP BY.

The function is named “injective” if it always returns different result for different values of arguments. In other words: different arguments never yield identical result.

**Syntax**

```
concatAssumeInjective(s1, s2, ...)

```

**Arguments**

- `s1`, `s2` ... – Values of type String or FixedString.

**Returned values**

- The String that results from concatenating the arguments.

Note: If any of argument values is `NULL` , `concatAssumeInjective` returns `NULL` .

**Example**

Input table:

```
CREATE TABLE test.key_val(`key1` String, `key2` String, `value` UInt32) ENGINE = CnchMergeTree ORDER BY key2;
INSERT INTO test.key_val VALUES ('Hello, ','World',1), ('Hello, ','World',2), ('Hello, ','World!',3), ('Hello',', World!',2);
SELECT * from test.key_val;

```

```
┌─key1───┬─key2─────┬─value─┐
│ Hello  │ , World! │ 2     │
│ Hello, │ World    │ 1     │
│ Hello, │ World    │ 2     │
│ Hello, │ World!   │ 3     │
└────────┴──────────┴───────┘

```

Query:

```
SELECT concat(key1, key2), sum(value) FROM test.key_val GROUP BY concatAssumeInjective(key1, key2);

```

Result:

```
┌─concat(key1, key2)─┬─sum(value)─┐
│ Hello,World        │ 3          │
│ Hello,World!       │ 3          │
│ Hello, World!      │ 2          │
└────────────────────┴────────────┘

```

## convertCharset

Returns the `string` that was converted from the encoding in `from` to the encoding in `to`.

**Syntax**

```
convertCharset(s, from, to)

```

**Arguments**

- `string` – The string.
- `from` – Decoding from a character encoding type.
- `to` – Encoding to a character encoding type.

**Returned value**

- `to` character encoding type.

**Example**

```
SELECT base64Encode(toString(convertCharset('abc', 'Unicode', 'UTF-8')))

```

Result:

```
┌─base64Encode(toString(convertCharset('abc', 'Unicode', 'UTF-8')))─┐
│ 5oWi77+9                                                          │
└───────────────────────────────────────────────────────────────────┘

```

## count

Counts the number of rows or not-NULL values.

**Syntax**

CNCH supports the following syntaxes for `count` :

- `count(expr)` or `COUNT(DISTINCT expr)` .
- `count()` or `COUNT(*)` .

**Arguments**

The function can take:

- Zero parameters.
- One expression .

**Returned value**

- If the function is called without parameters it counts the number of rows.
- If the expression is passed, then the function counts how many times this expression returned not null. If the expression returns a Nullable -type value, then the result of `count` stays not `Nullable` . The function returns 0 if the expression returned `NULL` for all the rows.
- In both cases the type of the returned value is UInt64.

**Details**

CNCH supports the `COUNT(DISTINCT ...)` syntax. The behavior of this construction depends on the count_distinct_implementation setting. It defines which of the uniq* functions is used to perform the operation. The default is the uniqExact function.

The `SELECT count() FROM table` query is not optimized, because the number of entries in the table is not stored separately. It chooses a small column from the table and counts the number of values in it.

However `SELECT count(nullable_column) FROM table` query can be optimized by enabling the optimize_functions_to_subcolumns setting. With `optimize_functions_to_subcolumns = 1` the function reads only null subcolumn instead of reading and processing the whole column data. The query `SELECT count(n) FROM table` transforms to `SELECT sum(NOT n.null) FROM table` .

**Examples**

Example 1:

```
CREATE TABLE test.test_count (id Int32) ENGINE = CnchMergeTree ORDER BY id;
INSERT INTO test.test_count(id) VALUES(1),(2),(3),(4),(5),(5);
select count() from test.test_count;

```

```
┌─count()─┐
│ 6       │
└─────────┘

```

Example 2:

```
SELECT name, value FROM system.settings WHERE name = 'count_distinct_implementation';

```

```
┌─name──────────────────────────┬─value─────┐
│ count_distinct_implementation │ uniqExact │
└───────────────────────────────┴───────────┘

```

```
select count(distinct(id)) from test.`test_count`;

```

```
┌─uniqExact(id)─┐
│ 5             │
└───────────────┘

```

This example shows that `count(DISTINCT num)` is performed by the `uniqExact` function according to the `count_distinct_implementation` setting value.

## e

Returns a Float64 number that is close to the number e.

**Syntax**

```
e()

```

**Arguments**

- N.A

**Returned value**

- Returns a Float64 number that is close to the number e.

Type: `Float64`

**Example**

```
SELECT e();

```

Result:

```
┌─e()───────────────────┐
│ 2.718281828459045e+00 │
└───────────────────────┘

```

## empty

Returns 1 for an empty string or 0 for a non-empty string.

A string is considered non-empty if it contains at least one byte, even if this is a space or a null byte.

The function also works for arrays.

**Syntax**

```
empty(string)

```

**Arguments**

- `string` – The string.

**Returned value**

- Returns 1 for an empty string or 0 for a non-empty string.

Type: `UInt8`

**Example**

```
SELECT empty('');

```

Result:

```
┌─empty('')─┐
│ 1         │
└───────────┘

```

Other example

```
SELECT empty('test');

```

Result:

```
┌─empty('12312')─┐
│ 0              │
└────────────────┘

```

## endsWith

Returns whether to end with the specified suffix. Returns 1 if the `string` ends with the specified `suffix`, otherwise it returns 0.

**Syntax**

```
endsWith(string, suffix)

```

**Arguments**

- `string` – The string.
- `suffix` – The suffix, test if `s` ends with the specified suffix.

**Returned value**

- Returns 1 if the string ends with the specified suffix, otherwise it returns 0.

Type: `UInt8`

**Example**

```
SELECT endsWith('test_end_with','with');

```

Result:

```
┌─endsWith('test_end_with', 'with')─┐
│ 1                                 │
└───────────────────────────────────┘

```

Other example

```
SELECT endsWith('test_end_with','error');

```

Result:

```
┌─endsWith('test_end_with', 'error')─┐
│ 0                                  │
└────────────────────────────────────┘

```

## extract

Extracts a fragment of a string using a regular expression. If `string` does not match the `pattern` regex, an empty string is returned. If the regex does not contain subpatterns, it takes the fragment that matches the entire regex. Otherwise, it takes the fragment that matches the first subpattern.

**Syntax**

```
extract(haystack, pattern)

```

**Arguments**

- `string` – The string.
- `pattern` – The regular expression pattern

**Returned value**

- The matched string.

Type: `string`

**Example**

```
SELECT extract('abc<regex1>abc<regex2>abc','<.*?>');

```

Result:

```
┌─extract('abc<regex1>abc<regex2>', '<.*?>')─┐
│ <regex1>                                   │
└────────────────────────────────────────────┘

```

## extractAll

Extracts all the fragments of a string using a regular expression. If `string` does not match the `pattern` regex, an empty string is returned. Returns an array of strings consisting of all matches to the regex. In general, the behavior is the same as the `extract` function (it takes the first subpattern, or the entire expression if there isn’t a subpattern).

**Syntax**

```
extractAll(string, pattern)

```

**Arguments**

- `string` – The string.
- `pattern` – The regular expression pattern

**Returned value**

- The matched string.

Type: `string`

**Example**

```
SELECT extractAll('abc<regex1>abc<regex2>abc','<.*?>');

```

Result:

```
┌─extractAll('abc<regex1>abc<regex2>abc', '<.*?>')─┐
│ [<regex1>, <regex2>]                             │
└──────────────────────────────────────────────────┘

```

## lcase

Converts ASCII Latin symbols in a `string` to lowercase.

**Syntax**

```
lcase(string)

```

**Arguments**

- `string` – The string.

**Returned value**

- The string in lowercase letter.

Type: `string`

**Example**

```
SELECT lcase('ABCdef');

```

Result:

```
┌─lcase('ABCdef')─┐
│ abcdef          │
└─────────────────┘

```

## length

Returns the length of a string in integer,also the function works for arrays.

**Syntax**

```
length(x)

```

**Arguments**

- `x` – The string or array.

**Returned value**

- The length of a string or array.

Type: `UInt64`

**Example**

```
SELECT length('Hello');

```

Result:

```
┌─length('Hello')─┐
│ 5               │
└─────────────────┘

```

Other example

```
SELECT length([1,2,3,4]);

```

Result:

```
┌─length([1, 2, 3, 4])─┐
│ 4                    │
└──────────────────────┘

```

## lengthUTF8

Returns the length of a string in integer, assuming that the string contains a set of bytes that make up UTF-8 encoded text. If this assumption is not met, it returns some result (it does not throw an exception).

**Syntax**

```
lengthUTF8(x)

```

**Arguments**

- `x` – The string or array.

**Returned value**

- The length of a string or array.

Type: `UInt64`

**Example**

encodeing 'Hello test' to UTF-8, we can get `\x48\x65\x6C\x6C\x6F\x20\x74\x65\x73\x74`

```
SELECT lengthUTF8('\x48\x65\x6C\x6C\x6F\x20\x74\x65\x73\x74');

```

Result:

```
┌─lengthUTF8('Hello test')─┐
│ 10                       │
└──────────────────────────┘

```

## like

like(string, pattern), string LIKE pattern operator checks whether a string matches a simple regular expression.

**Syntax**

```
like(string, pattern)
string LIKE pattern

```

**Arguments**

- `string` – The string.
- `pattern` – The string matches a simple regular expression.

The regular expression can contain the metasymbols `%` and `_` .

`%` indicates any quantity of any bytes (including zero characters).

`_` indicates any one byte.

Use the backslash ( `\` ) for escaping metasymbols. See the note on escaping in the description of the ‘match’ function.

For regular expressions like `%needle%` , the code is more optimal and works as fast as the `position` function.

For other regular expressions, the code is the same as for the ‘match’ function.

**Returned value**

- Either 1 or 0.

Type:`UInt8`

**Example**

```
SELECT like('abc','ab');

```

Type

Result:

```
┌─like('abc', 'ab')─┐
│ 0                 │
└───────────────────┘

```

Other example

```
SELECT 'abc' LIKE 'ab%';

```

Result:

```
┌─like('abc', 'ab%')─┐
│ 1                  │
└────────────────────┘

```

## locate

The locate() function returns the position of the first occurrence of a substring in a `string`.

**Alias** for position(string, substring)

**Syntax**

```
`locate(string, substring[, start_pos])` .

```

**Arguments**

- `string` – The String, in which substring will to be searched.String .
- `substring` – The Substring to be searched.String
- `start_pos` – The Optional parameter, position of the first character in the string to start search.UInt

**Returned value**

- the position of the first occurrence of a substring in a string.

Type: `UInt64`

**Example**

```
SELECT locate('Hello World', 'Wor');

```

Result:

```
┌─locate('Hello World', 'Wor')─┐
│ 7                            │
└──────────────────────────────┘

```

TODO: need to verify it after resolved [Locate does not support start_pos] [https://jira-sg.bytedance.net/browse/BYT-3176](https://jira-sg.bytedance.net/browse/BYT-3176), this task will be postpone till the final document on function part is ready

## lower

Converts ASCII Latin symbols in a string to lowercase.

**Syntax**

```
lower(string)

```

**Arguments**

- `string` – The string.

**Returned value**

- The string in lowercase letter.

Type: `string`

**Example**

```
SELECT lower('ABCdef');

```

Result:

```
┌─lower('ABCdef')─┐
│ abcdef          │
└─────────────────┘

```

## lowerUTF8

Converts a string to lowercase, assuming the string contains a set of bytes that make up a UTF-8 encoded text.

Note: It does not detect the language. So for Turkish the result might not be exactly correct.

If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.

If the string contains a set of bytes that is not UTF-8, then the behavior is undefined.

**Syntax**

```
lowerUTF8(s)

```

**Arguments**

- `s` – The string.

**Returned value**

- The string in lowercase letter.

Type: `string`

**Example**

encodeing 'Hello test' to UTF-8, we can get '\x48\x65\x6C\x6C\x6F\x20\x74\x65\x73\x74'.

```
SELECT lowerUTF8('\x48\x65\x6C\x6C\x6F\x20\x74\x65\x73\x74');

```

Result:

```
┌─lowerUTF8('Hello test')─┐
│ hello test              │
└─────────────────────────┘

```

## match

Checks whether the string matches the `pattern` regular expression. A `re2` regular expression. The [syntax](https://github.com/google/re2/wiki/Syntax) of the `re2` regular expressions is more limited than the syntax of the Perl regular expressions.

Note that the backslash symbol ( `\` ) is used for escaping in the regular expression. The same symbol is used for escaping in string literals. So in order to escape the symbol in a regular expression, you must write two backslashes () in a string literal.

The regular expression works with the string as if it is a set of bytes. The regular expression can’t contain null bytes.

For patterns to search for substrings in a string, it is better to use LIKE or ‘position’, since they work much faster.

**Syntax**

```
match(string, pattern)

```

**Arguments**

- `string` – The string.
- `pattern` – The pattern for matching

**Returned value**

- Returns 0 if it does not match, or 1 if it matches.

Type: `UInt8`

**Example**

```
SELECT match('abc<regex1>abc<regex2>abc','<.*?>');

```

Result:

```
┌─match('abc<regex1>abc<regex2>abc', '<.*?>')─┐
│ 1                                           │
└─────────────────────────────────────────────┘

```

## mid

Returns a substring starting from the ‘offset’ index that is ‘length’ long. Character indexing starts from one (as in standard SQL). The ‘offset’ and ‘length’ arguments must be constants.

**Syntax**

```
mid(s, offset, length)

```

**Arguments**

- `s` – The string.
- `offset` – The substring starting offset.
- `length` – The length of substring.

**Returned value**

- The substring.

Type: `string`

**Example**

```
SELECT mid('Hello CNCH',7,4);

```

Result:

```
┌─mid('Hello CNCH', 7, 4)─┐
│ CNCH                    │
└─────────────────────────┘

```

## multiFuzzyMatchAny

The same as `multiMatchAny` , but returns 1 if any pattern matches the string within a constant [edit distance](https://en.wikipedia.org/wiki/Edit_distance) . This function is also in an experimental mode and can be extremely slow. For more information see [hyperscan documentation](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching) .

**Syntax**

```
multiFuzzyMatchAny(string, distance, [pattern_1, pattern_2, …, pattern_n])

```

'''**Arguments**

- `string` – The string.
- `distance` – The integer, please refer to [edit distance](https://en.wikipedia.org/wiki/Edit_distance).
- `pattern` – The pattern using in fuzzy match.

**Returned value**

- returns 1 if any pattern matches the string, otherwise 0.

Type: `UInt8`

**Example**

```
SELECT multiFuzzyMatchAny('123ab12cdef',2,['abcd']);

```

Note: the distince is 2 due to the length of `12` in `ab12cd`

Result:

```
┌─multiFuzzyMatchAny('123ab12cdef', 2, ['abcd'])─┐
│ 1                                              │
└────────────────────────────────────────────────┘

```

## multiFuzzyMatchAnyIndex

The same as `multiFuzzyMatchAny` , but returns any index that matches the string within a constant edit distance.

**Syntax**

```
multiFuzzyMatchAnyIndex(string, distance, [pattern_1, pattern_2, …, pattern_n])

```

'''**Arguments**

- `string` – The string.
- `distance` – The integer, please refer to [edit distance](https://en.wikipedia.org/wiki/Edit_distance).
- `pattern` – The pattern using in fuzzy match.

**Returned value**

- returns returns any index that matches the string.

Type: `UInt64`

**Example**

```
SELECT multiFuzzyMatchAnyIndex('111333444CN1CH',1,['abc','def','CNCH']);

```

Note: the distince is 1 due to the length of `1` in `CN1CH`

Result:

```
┌─multiFuzzyMatchAnyIndex('111333444CN1CH', 1, ['abc', 'def', 'CNCH'])─┐
│ 3                                                                    │
└──────────────────────────────────────────────────────────────────────┘

```

## multiMatchAny

multiMatchAny(haystack, [pattern_1, pattern_2, …, pattern_n])

The same as `match` , but returns 0 if none of the regular expressions are matched and 1 if any of the patterns matches. It uses [hyperscan](https://github.com/intel/hyperscan) library. For patterns to search substrings in a string, it is better to use `multiSearchAny` since it works much faster.

Note: The length of any of the `haystack` string must be less than 232 bytes otherwise the exception is thrown. This restriction takes place because of hyperscan API.

**Syntax**

```
multiMatchAny(string, [pattern_1, pattern_2, …, pattern_n])

```

**Arguments**

- `string` – The string.
- `pattern` – The pattern for matching

**Returned value**

- Returns 0 if it does not match, or 1 if it matches.

Type: `UInt8`

**Example**

```
SELECT multiMatchAny('abc<regex1>abc<regex2>abc',['<.*?>','error_patten']);

```

Result:

```
┌─multiMatchAny('abc<regex1>abc<regex2>abc', ['<.*?>', 'error_patten'])─┐
│ 1                                                                     │
└───────────────────────────────────────────────────────────────────────┘

```

## multiMatchAnyIndex

The same as `multiMatchAny` , but returns any index that matches the string.

**Syntax**

```
multiMatchAnyIndex(string, [pattern_1, pattern_2, …, pattern_n])

```

**Arguments**

- `string` – The string.
- `pattern` – The pattern for matching

**Returned value**

- Returns any index that matches the string.

Type: `UInt64`

**Example**

```
SELECT multiMatchAnyIndex('abc<regex1>abc<regex2>abc',['error_patten', '<.*?>']);

```

Result:

```
┌─multiMatchAnyIndex('abc<regex1>abc<regex2>abc', ['error_patten', '<.*?>'])─┐
│ 2                                                                          │
└────────────────────────────────────────────────────────────────────────────┘

```

## multiSearchAllPositions

The same as position but returns `Array` of positions of the found corresponding substrings in the string. Positions are indexed starting from 0.

The search is performed on sequences of bytes without respect to string encoding and collation.

- For case-insensitive ASCII search, use the function `multiSearchAllPositionsCaseInsensitive` .
- For search in UTF-8, use the function [multiSearchAllPositionsUTF8](https://bytedance.feishu.cn/docs/doccnUTIuxNrSuriPrZLQgGzmob#multiSearchAllPositionsUTF8) .
- For case-insensitive UTF-8 search, use the function multiSearchAllPositionsCaseInsensitiveUTF8.

**Syntax**

```
multiSearchAllPositions(string, [substring_1, substring_2, ..., Substring_n])

```

**Arguments**

- `string` — The string, in which substring will to be searched. String .
- `substring` — Substring to be searched. String .

**Returned values**

- Array of starting positions in (counting from 1), if the corresponding substring was found and 0 if not found.

Type: `Array(UInt64)`

**Example**

```
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world']);

```

Result:

```
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0, 13, 0]                                                        │
└───────────────────────────────────────────────────────────────────┘

```

## multiSearchAllPositionsUTF8

Please refer to `multiSearchAllPositions` .## multiSearchAny
Returns 1, if at least one string needle_i matches the string `string` and 0 otherwise.

For a case-insensitive search or/and in UTF-8 format use functions `multiSearchAnyCaseInsensitive, multiSearchAnyUTF8, multiSearchAnyCaseInsensitiveUTF8`.

Note:

In all `multiSearch*` functions the number of patterns should be less than 28 because of implementation specification.

**Syntax**

```
multiSearchAny(string, [pattern_1, pattern_2, …, pattern_n]) 

```

**Arguments**

- `string` – The string.
- `pattern` – The pattern for matching

**Returned value**

- Returns 1, if at least one pattern matches the string `string` and 0 otherwise.

Type: `UInt8`

**Example**

```
SELECT multiSearchAny('can_you_find_CNCH?',['abc','CNCH']);

```

Result:

```
┌─multiSearchAny('can_you_find_CNCH?', ['abc', 'CNCH'])─┐
│ 1                                                     │
└───────────────────────────────────────────────────────┘

```

## multiSearchFirstIndex

Returns the index `i` (starting from 1) of the leftmost found pattern_i in the `string` and 0 otherwise.

For a case-insensitive search or/and in UTF-8 format use functions `multiSearchFirstIndexCaseInsensitive, multiSearchFirstIndexUTF8, multiSearchFirstIndexCaseInsensitiveUTF8` .

**Syntax**

```
multiSearchFirstIndex(string, [pattern_1, pattern_2, …, pattern_n]) 

```

**Arguments**

- `string` – The string.
- `pattern` – The pattern for matching

**Returned value**

- Returns the index `i` (starting from 1) of the leftmost found pattern_i in the `string` and 0 otherwise.

Type: `UInt8`

**Example**

```
SELECT multiSearchFirstIndex('which_pattern_matchs_CNCH?',['abc','CNCH']);

```

Result:

```
┌─multiSearchFirstIndex('which_pattern_matchs_CNCH?', ['abc', 'CNCH'])─┐
│ 2                                                                    │
└──────────────────────────────────────────────────────────────────────┘

```

## multiSearchFirstPosition

The same as `position` but returns the leftmost offset of the string that is matched to some of the substring.

For a case-insensitive search or/and in UTF-8 format use functions `multiSearchFirstPositionCaseInsensitive, multiSearchFirstPositionUTF8, multiSearchFirstPositionCaseInsensitiveUTF8` .

**Syntax**

```
multiSearchFirstPosition(string, [pattern_1, pattern_2, …, pattern_n]) 

```

**Arguments**

- `string` – The string.
- `substring` – The substring for matching.

**Returned value**

- Returns the index `i` (starting from 1) of the leftmost found pattern_i in the `string` and 0 otherwise.

Type: `UInt8`

**Example**

```
SELECT multiSearchFirstPosition('123_abc_abc_CNCH?',['abc','CNCH']);

```

Result:

```
┌─multiSearchFirstPosition('123_abc_abc_CNCH?', ['abc', 'CNCH'])─┐
│ 5                                                              │
└────────────────────────────────────────────────────────────────┘

```

## ngramDistance

Calculates the 4-gram distance between `string` and `substring` : counts the symmetric difference between two multisets of 4-grams and normalizes it by the sum of their cardinalities. Returns float number from 0 to 1 – the closer to zero, the more strings are similar to each other. If the constant `substring` or `substring` is more than 32Kb, throws an exception. If some of the non-constant `substring` or `substring` strings are more than 32Kb, the distance is always one.

For case-insensitive search or/and in UTF-8 format use functions `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8` .

**Syntax**

```
ngramDistance(string, [substring_1, substring_2, …, substring_n]) 

```

**Arguments**

- `string` – The string.
- `substring` – The substring for matching.

**Returned value**

- Returns the index `i` (starting from 1) of the leftmost found substring_i in the `string` and 0 otherwise.

Type: `Float32`

**Example**

```
SELECT ngramDistance('abc123ascCNCH_83q','CNCH');

```

Result:

```
┌─ngramDistance('abc123ascCNCH_83q', 'CNCH')─┐
│ 8.666667e-01                               │
└────────────────────────────────────────────┘

```

## not

Calculates the result of the logical negation of the value. Corresponds to Logical Negation Operator .

**Syntax**

```
not(val);

```

**Arguments**

- `val` — The value. Int , UInt , Float or Nullable .

**Returned value**

- `1` , if the `val` is `0` .
- `0` , if the `val` is a non-zero value.
- `NULL` , if the `val` is a `NULL` value.

Type: UInt8 or Nullable ( UInt8.

**Example**

```
SELECT NOT(1);

```

Result:

```
┌─not(1)─┐
│ 0      │
└────────┘

```

## notEmpty

Returns 0 for an empty `string` or 1 for a non-empty `string`.

The function also works for arrays.

**Syntax**

```
notEmpty(string)

```

**Arguments**

- `s` – The string.

**Returned value**

- Returns 1 for an empty string or 0 for a non-empty string.

Type: `UInt8`

**Example**

```
SELECT notEmpty('test');

```

Result:

```
┌─notEmpty('test')─┐
│ 1                │
└──────────────────┘

```

Other example

```
SELECT notEmpty('');

```

Result:

```
┌─notEmpty('')─┐
│ 0            │
└──────────────┘

```

## notLike

notLike(string, pattern), string NOT LIKE pattern operator.

The same thing as ‘like’, but negative.

**Syntax**

```
notLike(string, pattern)

```

**Arguments**

- `string` – The string.
- `pattern` – The pattern for matching.

**Returned value**

- Returns 0 if it does not match, or 1 if it matches.

Type: `UInt8`

**Example**

```
SELECT notLike('test1','test2');

```

Result:

```
┌─notLike('test1', 'test2')─┐
│ 1                         │
└───────────────────────────┘

```

## position

Searches for the `substring` in the `string`.

Returns the position of the found substring in the string, starting from 1.

Note: The search is case-sensitive by default, for a case-insensitive search, use the function positionCaseInsensitive .

**Syntax**

```
position(substring IN string)

position(string, substring[, start_pos])

```

Alias: `locate(string, substring[, start_pos])` .

Note: Syntax of `position(substring IN string)` provides SQL-compatibility, the function works the same way as to `position(string, substring)` .

**Arguments**

- `string` – The String, in which substring will to be searched.String .
- `substring` – The Substring to be searched.String
- `start_pos` – The Optional parameter, position of the first character in the string to start search.UInt

**Returned values**

- Returns the position of the found substring in the string, starting from 1.

Type: `Integer`

**Examples**

The phrase “Hello, world!” contains a set of bytes representing a single-byte encoded text. The function returns some expected result:

```
SELECT position('Hello, world!', '!');

```

Result:

```
┌─position('Hello, world!', '!')─┐
│ 13                             │
└────────────────────────────────┘

```

TODO: need to verify it after resolved [Locate does not support start_pos] [https://jira-sg.bytedance.net/browse/BYT-3176](https://jira-sg.bytedance.net/browse/BYT-3176), this task will be postpone till the final document on function part is ready

```
SELECT position('Hello, world!', 'o', 1),position('Hello, world!', 'o', 7);

```

```
┌─position('Hello, world!', 'o', 1)─┬─position('Hello, world!', 'o', 7)─┐

│                                 5 │                                 9 │

└───────────────────────────────────┴───────────────────────────────────┘

```

The same phrase in Russian contains characters which can’t be represented using a single byte. The function returns some unexpected result (use [positionUTF8](https://bytedance.feishu.cn/docs/doccnUTIuxNrSuriPrZLQgGzmob#positionutf8) function for multi-byte encoded text):

```
SELECT position('Привет, мир!', '!');

```

Result:

```
┌─position('Привет, мир!', '!')─┐
│                            21 │
└───────────────────────────────┘

```

**Examples for position(substring IN string) syntax**

```
SELECT 3 = position('c' IN 'abc');

```

Result:

```
┌─equals(3, position('abc', 'c'))─┐
│                               1 │
└─────────────────────────────────┘

```

Query:

```
SELECT 6 = position('/' IN s) FROM (SELECT 'Hello/World' AS s);

```

Result:

```
┌─equals(6, position(s, '/'))─┐
│                           1 │
└─────────────────────────────┘

```

## positionCaseInsensitive

The same as position returns the position of the found substring in the string, starting from 1. Use the function for a case-insensitive search.

Works under the assumption that the string contains a set of bytes representing a single-byte encoded text. If this assumption is not met and a character can’t be represented using a single byte, the function does not throw an exception and returns some unexpected result. If character can be represented using two bytes, it will use two bytes and so on.

**Syntax**

```
positionCaseInsensitive(string, substring[, start_pos])

```

**Arguments**

- `string` – The String, in which substring will to be searched.String .
- `substring` – The Substring to be searched.String
- `start_pos` – The Optional parameter, position of the first character in the string to start search.UInt

**Returned values**

- Starting position in bytes (counting from 1), if substring was found.
- 0, if the substring was not found.

Type: `Integer` .

**Example**

```
SELECT positionCaseInsensitive('Hello, world!', 'hello');

```

Result:

```
┌─positionCaseInsensitive('Hello, world!', 'hello')─┐
│ 1                                                 │
└───────────────────────────────────────────────────┘

```

TODO: need to verify it after resolved [Locate does not support start_pos] [https://jira-sg.bytedance.net/browse/BYT-3176](https://jira-sg.bytedance.net/browse/BYT-3176), this task will be postpone till the final document on function part is ready

## positionCaseInsensitiveUTF8

The same as positionUTF8 , but is case-insensitive. Returns the position (in Unicode points) of the found `substring` in the `string`, starting from 1.

Works under the assumption that the string contains a set of bytes representing a UTF-8 encoded text. If this assumption is not met, the function does not throw an exception and returns some unexpected result. If character can be represented using two Unicode points, it will use two and so on.

**Syntax**

```
positionCaseInsensitiveUTF8(string, substring[, start_pos])

```

**Arguments**

- `string` – The String, in which substring will to be searched.String .
- `substring` – The Substring to be searched.String
- `start_pos` – The Optional parameter, position of the first character in the string to start search.UInt

**Returned value**

- Starting position in Unicode points (counting from 1), if substring was found.
- 0, if the substring was not found.

Type: `Integer` .

**Example**

Query:

```
SELECT positionCaseInsensitiveUTF8('Привет, мир!', 'Мир');

```

Result:

```
┌─positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')─┐
│ 9                                                  │
└────────────────────────────────────────────────────┘

```

## positionUTF8

Returns the position (in Unicode points) of the found `substring` in the `string`, starting from 1.

Works under the assumption that the string contains a set of bytes representing a UTF-8 encoded text. If this assumption is not met, the function does not throw an exception and returns some unexpected result. If character can be represented using two Unicode points, it will use two and so on.

For a case-insensitive search, use the function positionCaseInsensitiveUTF8 .

**Syntax**

```
positionUTF8(string, substring[, start_pos])

```

**Arguments**

- `string` – The String, in which substring will to be searched.String .
- `substring` – The Substring to be searched.String
- `start_pos` – The Optional parameter, position of the first character in the string to start search.UInt

**Returned values**

- Starting position in Unicode points (counting from 1), if substring was found.
- 0, if the substring was not found.

Type: `Integer` .

**Examples**

```
SELECT positionUTF8('Привет, мир!', '!') as example;

```

The phrase “Hello, world!” in Russian contains a set of Unicode points representing a single-point encoded text. The function returns some expected result:

Result:

```
┌─example─┐
│ 12      │
└─────────┘

```

TODO: Xinghe: below sentences will be deleted due to two query returns the same result.

The phrase “Salut, étudiante!”, where character `é` can be represented using a one point ( `U+00E9` ) or two points ( `U+0065U+0301` ) the function can be returned some unexpected result:

Query for the letter `é` , which is represented one Unicode point `U+00E9` :

```
SELECT positionUTF8('Salut, étudiante!', '!');

```

Result:

```
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     17 │
└────────────────────────────────────────┘

```

Query for the letter `é` , which is represented two Unicode points `U+0065U+0301` :

```
SELECT positionUTF8('Salut, étudiante!', '!');

```

Result:

```
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     18 │
└────────────────────────────────────────┘

```

## regexpQuoteMeta

The function adds a backslash before some predefined characters in the `string`.

Predefined characters: `\0` , `|` , `(` , `)` , `^` , `$` , `.` , `[` , `]` , `?` , `*` , `+` , `{` , `:` , `-` .

TODO: Note: i have deleted `\\` in predefined characters because it's not working.

This implementation slightly differs from re2::RE2::QuoteMeta. It escapes zero byte as `\0` instead of `\x00` and it escapes only required characters.

For more information, see the link: [RE2](https://github.com/google/re2/blob/master/re2/re2.cc#L473)

**Syntax**

```
regexpQuoteMeta(string)

```

**Arguments**

- `string` – The string.

**Returned value**

– The string.

Type: `string`

**Example**

```
SELECT regexpQuoteMeta(' | , ( , ) , ^ , $ , . , [ , ] , ? ,');

```

Result:

```
┌─regexpQuoteMeta(' | , ( , ) , ^ , $ , . , [ , ] , ? ,')─┐
│  \| , \( , \) , \^ , \$ , \. , \[ , \] , \? ,           │
└─────────────────────────────────────────────────────────┘

```

## replace

Replaces all occurrences of the `substring` in `string` with the `replacement`.

Alias to replaceAll

**Syntax**

```
replace(string, substring, replacement)

```

**Arguments**

- `string` — The String, in which substring will to be searched.
- `substring` — The Substring to be searched.
- `replacement` — The replacement string to replace matched substring.

**Returned value**

- Replaces all occurrences of the `substring` in `string` with the `replacement`.

Type: `string`

**Example**

```
SELECT replace('test target_string test','target_string','CNCH');

```

Result:

```
┌─replace('test target_string test', 'target_string', 'CNCH')─┐
│ test CNCH test                                              │
└─────────────────────────────────────────────────────────────┘

```

## replaceAll

Replaces all occurrences of the `substring` in `string` with the `replacement`.

Please refer to replace

## replaceOne

Replaces the first occurrence, if it exists, of the `substring` in `string` with the `replacement`.

**Syntax**

```
replaceOne(string, substring, replacement)

```

**Arguments**

- `string` – The String, in which substring will to be searched.String .
- `substring` – The Substring to be searched.String
- `start_pos` – The Optional parameter, position of the first character in the string to start search.UInt

**Returned value**

- Replaces first occurrences of the substring in string with the replacement.

Type: `string`

**Example**

```
SELECT replaceOne('test target_string test target_string','target_string','CNCH');

```

Result:

```
┌─replaceOne('test target_string test target_string', 'target_string', 'CNCH')─┐
│ test CNCH test target_string                                                 │
└──────────────────────────────────────────────────────────────────────────────┘

```

## replaceRegexpAll

This does the same thing like replaceAll, but using regular expression.

- A pattern can be specified as ‘replacement’. This pattern can include substitutions `\0-\9` .
- The substitution `\0` includes the entire regular expression. Substitutions `\1-\9` correspond to the subpattern numbers.To use the `\` character in a template, escape it using `\` .

Noted: Replacement using the re2 regular expression.

**Syntax**

```
replaceRegexpAll(string, pattern, replacement)

```

**Arguments**

- `string` – The string.
- `pattern` – The pattern for matching.
- `replacement` — The replacement string to replace matched substring.

**Returned value**

- Replaces all occurrences of the substring in string with the replacement.

Type: `string`

**Example**

```
SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0');

```

```
┌─replaceRegexpAll('Hello, World!', '.', '\\0\\0')─┐
│ HHeelllloo,,  WWoorrlldd!!                       │
└──────────────────────────────────────────────────┘

```

As an exception, if a regular expression worked on an empty substring, the replacement is not made more than once.

Example:

```
SELECT replaceRegexpAll('Hello, World!', '^', 'here: ');

```

```
┌─replaceRegexpAll('Hello, World!', '^', 'here: ')─┐
│ here: Hello, World!                              │
└──────────────────────────────────────────────────┘

```

## replaceRegexpOne

Replaces only the first occurrence, if it exists.

Noted: Replacement using the pattern regular expression. A re2 regular expression.

A pattern can be specified as ‘replacement’. This pattern can include substitutions `\0-\9` .

The substitution `\0` includes the entire regular expression. Substitutions `\1-\9` correspond to the subpattern numbers.To use the `\` character in a template, escape it using `\` .

Also keep in mind that a string literal requires an extra escape.

**Syntax**

```
replaceRegexpOne(string, pattern, replacement)

```

**Arguments**

- `string` – The string.
- `pattern` – The pattern for matching.
- `replacement` — The replacement string to replace matched substring.

**Returned value**

- Replaces first occurrences of the substring in string with the replacement.

Type: `string`

**Example**

```
SELECT replaceRegexpOne('test <target_string> test <target_string>','<.*?>','CNCH');

```

Result:

```
┌─replaceRegexpOne('test <target_string> test <target_string>', '<.*?>', 'CNCH')─┐
│ test CNCH test <target_string>                                                 │
└────────────────────────────────────────────────────────────────────────────────┘

```

**other example**

Example 1. Converting the date to American format:

```
select '2014-03-17', replaceRegexpOne(toString('2014-03-17'), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1');

```

```
┌─'2014-03-17'─┬─replaceRegexpOne(toString('2014-03-17'), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1')─┐
│ 2014-03-17   │ 03/17/2014                                                                            │
└──────────────┴───────────────────────────────────────────────────────────────────────────────────────┘

```

Example 2. Copying a string ten times:

```
SELECT replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0');

```

Result:

```
┌─replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0')──────────────────────────────────────────────────────────┐
│ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

```

## reverse

Reverses the string (as a sequence of bytes).

**Syntax**

```
reverse(string)

```

**Arguments**

- `string` – The string.

**Returned value**

- The reversed string.

Type: `string`

**Example**

```
SELECT reverse('abcd1234');

```

Result:

```
┌─reverse('abcd1234')─┐
│ 4321dcba            │
└─────────────────────┘

```

## reverseUTF8

Reverses a sequence of Unicode code points, assuming that the string contains a set of bytes representing a UTF-8 text. Otherwise, it does something else (it does not throw an exception).

**Syntax**

```
reverseUTF8(string)

```

**Arguments**

- `string` – The UTF8 string.

**Returned value**

- The reversed string.

Type: `string`

**Example**

```
SELECT reverseUTF8('\x61\x62\x63\x64\x31\x32\x33\x34');

```

Note: '\x61\x62\x63\x64\x31\x32\x33\x34' is 'abcd1234' in UTF8 format.

Result:

```
┌─reverseUTF8('abcd1234')─┐
│ 4321dcba                │
└─────────────────────────┘

```

## size

Returns the length of a string in bytes (not in characters, and not in code points).

Noted: The function also works for arrays.

Alias to length

**Syntax**

```
size(x)

```

**Arguments**

- `x` – The string or array.

**Returned value**

- The length of a string or array.

Type: `UInt64`

**Example**

```
SELECT size('Hello');

```

Result:

```
┌─size('Hello')─┐
│ 5             │
└───────────────┘

```

Other example

```
SELECT size([1,2,3,4]);

```

Result:

```
┌─size([1, 2, 3, 4])─┐
│ 4                  │
└────────────────────┘

```

## splitByChar

Splits a string into substrings separated by a specified character. It uses a constant string `separator` which consisting of exactly one character.

Returns an array of selected substrings. Empty substrings may be selected if the separator occurs at the beginning or end of the string, or if there are multiple consecutive separators.

**Syntax**

```
splitByChar(separator, string)

```

**Arguments**

- `separator` — The separator which should contain exactly one character. String .
- `string` — The string to split. String .

**Returned value**

Returns an array of selected substrings. Empty substrings may be selected when:

- A separator occurs at the beginning or end of the string;
- There are multiple consecutive separators;
- The original string `string` is empty.

Type: `array`

**Example**

```
SELECT splitByChar(',', '1,2,3,abcde');

```

Result:

```
┌─splitByChar(',', '1,2,3,abcde')─┐
│ [1, 2, 3, abcde]                │
└─────────────────────────────────┘

```

## splitByString

Splits a string into substrings separated by a string. It uses a constant string `separator` of multiple characters as the separator. If the string `separator` is empty, it will split the `string` into an array of single characters.

**Syntax**

```
splitByString(separator, string)

```

**Arguments**

- `separator` — The separator which should contain a string. String .
- `string` — The string to split. String .

**Returned value**

Returns an array of selected substrings. Empty substrings may be selected when:

- A separator occurs at the beginning or end of the string;
- There are multiple consecutive separators;
- The original string `string` is empty.

Type: `array`

**Example**

```
SELECT splitByString('test', 'abctestCNCHtestdef');

```

Result:

```
┌─splitByString('test', 'abctestCNCHtestdef')─┐
│ [abc, CNCH, def]                            │
└─────────────────────────────────────────────┘

```

## startsWith

Returns 1 whether `string` starts with the specified `prefix`, otherwise it returns 0.

**Syntax**

```
startsWith(string, prefix)

```

**Arguments**

- `string` — The string .
- `prefix` — The prefix for matching.

**Returned values**

- 1, if the string starts with the specified prefix.
- 0, if the string does not start with the specified prefix.

Type: `UInt8`

**Example**

```
SELECT startsWith('Hello, world!', 'He');

```

Result:

```
┌─startsWith('Hello, world!', 'He')─┐
│ 1                                 │
└───────────────────────────────────┘

```

## substr

Please refer to substring## substring
Returns a substring starting with the byte from the `offset` index that is `length` bytes long. Character indexing starts from one (as in standard SQL).

Note: The `offset` and `length` arguments must be constants.

**Syntax**

```
substring(string, offset, length)

```

**Arguments**

- `string` – The string.
- `offset` – The substring starting offset.
- `length` – The length of substring.

**Returned value**

- The substring.

Type: `string`

**Example**

```
SELECT substring('Hello CNCH', 7, 4);

```

Result:

```
┌─substr('Hello CNCH', 7, 4)─┐
│ CNCH                       │
└────────────────────────────┘

```

## substringUTF8

The same as substring, but for Unicode code points. Works under the assumption that the string contains a set of bytes representing a UTF-8 encoded text.

Note: If this assumption is not met, it returns some result (it does not throw an exception).

**Syntax**

```
substringUTF8(string, offset, length)

```

**Arguments**

- `string` – The string.
- `offset` – The substring starting offset.
- `length` – The length of substring.

**Returned value**

- The substring.

Type: `string`

**Example**

```
SELECT substringUTF8('\x48\x65\x6c\x6c\x6f\x20\x43\x4e\x43\x48H', 7, 4);

```

Result:

```
┌─substringUTF8('Hello CNCHH', 7, 4)─┐
│ CNCH                               │
└────────────────────────────────────┘

```

## trimBoth

Removes all consecutive occurrences of common whitespace (ASCII character 32) from both ends of a string. It does not remove other kinds of whitespace characters (tab, no-break space, etc.).

**Syntax**

```
trimBoth(string)

```

Alias: `trim(string)` .

**Arguments**

- `string` — string to trim. String .

**Returned value**

- A string without leading and trailing common whitespaces.

Type: `String`

**Example**

```
SELECT trimBoth('     Hello, world!     ') as a;

```

Result:

```
┌─a─────────────┐
│ Hello, world! │
└───────────────┘

```

## trimLeft

Removes all consecutive occurrences of common whitespace (ASCII character 32) from the beginning of a string. It does not remove other kinds of whitespace characters (tab, no-break space, etc.).

**Syntax**

```
trimLeft(string)

```

Alias: `trim(string)` .

**Arguments**

- `string` — string to trim. String .

**Returned value**

- A string without leading common whitespaces.

Type: `String`

**Example**

```
SELECT trimLeft('     Hello, world!     ') as example;

```

Result:

```
┌─example────────────┐
│ Hello, world!      │
└────────────────────┘

```

## trimRight

Removes all consecutive occurrences of common whitespace (ASCII character 32) from the end of a string. It does not remove other kinds of whitespace characters (tab, no-break space, etc.).

**Syntax**

```
trimRight(string)

```

Alias: `trim(string)` .

**Arguments**

- `string` — string to trim. String .

**Returned value**

- A string without trailing common whitespaces.

Type: `String`

**Example**

TODO： below query will trims characters.... i have feedbacked to byteyard on call

```
SELECT trimRight('     Hello, world!     ');

```

Result:

```
┌─trimLeft('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘

```

## tryBase64Decode

Similar to base64Decode, but in case of error an empty string would be returned.

**Syntax**

```
tryBase64Decode(string)

```

**Arguments**

- `string` – The base64 encoded string.

**Returned value**

- The decoded string.

Type: `string`

**Example**

```
SELECT tryBase64Decode('SGVsbG8gV29ybGQh');

```

Note: 'SGVsbG8gV29ybGQh' is the encoded base64 format of 'Hello World! '

Result:

```
┌─tryBase64Decode('SGVsbG8gV29ybGQh')─┐
│ Hello World!                        │
└─────────────────────────────────────┘

```

## ucase

Converts ASCII Latin symbols in a string to uppercase.

**Syntax**

```
ucase(string)

```

**Arguments**

- `string` – The string.

**Returned value**

- The string in uppercase letter.

Type: `string`

**Example**

```
SELECT ucase('ABCdef');

```

Result:

```
┌─ucase('ABCdef')─┐
│ ABCDEF          │
└─────────────────┘

```

## upper

Converts ASCII Latin symbols in a string to uppercase.

**Syntax**

```
upper(string)

```

**Arguments**

- `string` – The string.

**Returned value**

- The string in uppercase letter.

Type: `string`

**Example**

```
SELECT upper('ABCdef');

```

Result:

```
┌─upper('ABCdef')─┐
│ ABCDEF          │
└─────────────────┘

```

## upperUTF8

Converts a string to uppercase, assuming the string contains a set of bytes that make up a UTF-8 encoded text.

- It does not detect the language. So for Turkish the result might not be exactly correct.
- If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.
- If the string contains a set of bytes that is not UTF-8, then the behavior is undefined.

**Syntax**

```
upperUTF8(string)

```

**Arguments**

- `string` – The UTF8 string.

**Returned value**

- The string in uppercase letter.

Type: `string`

**Example**

encode 'hello world ！' to UTF8 '\x68\x65\x6c\x6c\x6f\x20\x77\x6f\x72\x6c\x64\x20\xef\xbc\x81'

```
SELECT upperUTF8('\x68\x65\x6c\x6c\x6f\x20\x77\x6f\x72\x6c\x64\x20\xef\xbc\x81');

```

Result:

```
┌─upperUTF8('hello world ！')─┐
│ HELLO WORLD ！                │
└──────────────────────────────┘

```
