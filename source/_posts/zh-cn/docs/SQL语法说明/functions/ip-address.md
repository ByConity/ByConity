---
title: "IP Address"
slug: "ip-address"
hidden: false
createdAt: "2021-07-29T12:06:52.617Z"
updatedAt: "2021-09-23T04:04:27.184Z"
categories:
- Docs
- SQL_Syntax
tags:
- Docs
---
> Notice:
Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

## IPv4NumToString 
Takes a UInt32 number. Interprets it as an IPv4 address in big endian. Returns a string containing the corresponding IPv4 address in the format A.B.C.d (dot-separated numbers in decimal form).

**Syntax**

```sql
IPv4NumToString(num)
```

**Arguments**
- `num` – a UInt32 number.

**Returned value**
- A string in ipv4 representation.

**Examples**

```sql
SELECT toIPv4('116.106.34.242') as ipv4, toTypeName(ipv4), IPv4NumToString(ipv4) as ipv4_string, toTypeName(ipv4_string)
```

```plain%20text
┌─ipv4───────────┬─toTypeName(toIPv4('116.106.34.242'))─┬─ipv4_string────┬─toTypeName(IPv4NumToString(toIPv4('116.106.34.242')))─┐
│ 242.34.106.116 │ IPv4                                 │ 116.106.34.242 │ String                                                │
└────────────────┴──────────────────────────────────────┴────────────────┴───────────────────────────────────────────────────────┘
```

## IPv4NumToStringClassC
Similar to `IPv4NumToString`, but using xxx instead of the last octet.

**Syntax**

```sql
IPv4NumToStringClassC(num)
```

**Arguments**
- `num` – a UInt32 number.

**Returned value**
- An string in ipv4 representation, but using xxx instead of the last octet.

**Examples**

```sql
SELECT toIPv4('116.106.34.242') as ipv4, toTypeName(ipv4), IPv4NumToStringClassC(ipv4) as ipv4_string, toTypeName(ipv4_string)
```

```plain%20text
┌─ipv4───────────┬─toTypeName(toIPv4('116.106.34.242'))─┬─ipv4_string────┬─toTypeName(IPv4NumToStringClassC(toIPv4('116.106.34.242')))─┐
│ 242.34.106.116 │ IPv4                                 │ 116.106.34.xxx │ String                                                      │
└────────────────┴──────────────────────────────────────┴────────────────┴─────────────────────────────────────────────────────────────┘
```

## IPv4StringToNum
The reverse function of IPv4NumToString. If the IPv4 address has an invalid format, it returns 0.

**Syntax**

```sql
IPv4StringToNum(s)
```

**Arguments**
- `s` – ipv4 in string representation.

**Returned value**
- UInt32.

**Examples**

```sql
SELECT IPv4StringToNum('116.106.34.242') as ipv4, toTypeName(ipv4), IPv4NumToString(ipv4) as ipv4_string, toTypeName(ipv4_string)
```

```plain%20text
┌─ipv4───────┬─toTypeName(IPv4StringToNum('116.106.34.242'))─┬─ipv4_string────┬─toTypeName(IPv4NumToString(IPv4StringToNum('116.106.34.242')))─┐
│ 1953112818 │ UInt32                                        │ 116.106.34.242 │ String                                                         │
└────────────┴───────────────────────────────────────────────┴────────────────┴────────────────────────────────────────────────────────────────┘
```

## IPv4ToIPv6
Takes a `UInt32` number. Interprets it as an IPv4 address in [big endian](https://en.wikipedia.org/wiki/Endianness) . Returns a `FixedString(16)` value containing the IPv6 address in binary format.

**Syntax**

```sql
IPv4ToIPv6(x)
```

**Arguments**
- `x` – a `UInt32` number

**Returned value**
- IPv6 address in binary format.FixedString(16)

**Examples**

```sql
SELECT IPv4StringToNum('192.168.0.1') as ipv4, IPv6NumToString(IPv4ToIPv6(ipv4)) as ipv6_string
```

```plain%20text
┌─ipv4───────┬─ipv6_string────────┐
│ 3232235521 │ ::ffff:192.168.0.1 │
└────────────┴────────────────────┘
```

## IPv6NumToString
Accepts a FixedString(16) value containing the IPv6 address in binary format. Returns a string containing this address in text format.

IPv6-mapped IPv4 addresses are output in the format ::ffff:111.222.33.44.

**Syntax**

```sql
IPv6NumToString(x)
```

**Arguments**
- `x` – FixedString(16) value containing the IPv6 address in binary format

**Returned value**
- A string in ipv6 representation.

**Examples**

```sql
SELECT IPv6NumToString(toFixedString(unhex('2A0206B8000000000000000000000011'), 16)) AS addr;
```

```plain%20text
┌─addr─────────┐
│ 2a02:6b8::11 │
└──────────────┘
```

## IPv6StringToNum
The reverse function of `IPv6NumToString`. If the IPv6 address has an invalid format, it returns a string of null bytes.
If the input string contains a valid IPv4 address, returns its IPv6 equivalent.
HEX can be uppercase or lowercase.

**Syntax**

```sql
IPv6StringToNum(string)
```

**Argument** 
- `string` — IP address. String. 

**Returned value**
- IPv6 address in binary format. Type: FixedString(16).

**Example**

```sql
SELECT addr, cutIPv6(IPv6StringToNum(addr), 0, 0) FROM (SELECT ['notaddress', '127.0.0.1', '1111::ffff'] AS addr) ARRAY JOIN addr;
```

```plain%20text
┌─addr───────┬─cutIPv6(IPv6StringToNum(addr), 0, 0)─┐
│ notaddress │ ::                                   │
│ 127.0.0.1  │ ::                                   │
│ 1111::ffff │ 1111::ffff                           │
└────────────┴──────────────────────────────────────┘
```



## cutIPv6
Accepts a FixedString(16) value containing the IPv6 address in binary format. Returns a string containing the address of the specified number of bytes removed in text format.

**Syntax**

```sql
cutIPv6(x, bytesToCutForIPv6, bytesToCutForIPv4)
```

**Arguments**
- `x` – a FixedString(16) value containing the IPv6 address in binary format
- `bytesToCutForIPv6` - number of bytes to cut for ipv6 represenration
- `bytesToCutForIPv4` - number of bytes to cut for ipv4 represenration

**Returned value**
A `Uint64` data type hash value.

Type: `UInt64`

**Examples**

```sql
WITH
    IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D') AS ipv6,
    IPv4ToIPv6(IPv4StringToNum('192.168.0.1')) AS ipv4
SELECT
    cutIPv6(ipv6, 2, 0),
    cutIPv6(ipv4, 0, 2)
```

```plain%20text
┌─cutIPv6(ipv6, 2, 0)─────────────────┬─cutIPv6(ipv4, 0, 2)─┐
│ 2001:db8:ac10:fe01:feed:babe:cafe:0 │ ::ffff:192.168.0.0  │
└─────────────────────────────────────┴─────────────────────┘
```

## toIPv4
An alias to `IPv4StringToNum()` that takes a string form of IPv4 address and returns value of IPv4 type, which is binary equal to value returned by `IPv4StringToNum()` .

**Syntax**

```sql
toIPv4(string)
```

**Argument** 
- `string` — IP address. String.

**Returned value**
- IPv4 type

**Example**

```sql
WITH
    '171.225.130.45' as IPv4_string
SELECT
    toTypeName(IPv4StringToNum(IPv4_string)),
    toTypeName(toIPv4(IPv4_string))
```

```plain%20text
┌─toTypeName(IPv4StringToNum(IPv4_string))─┬─toTypeName(toIPv4(IPv4_string))─┐
│ UInt32                                   │ IPv4                            │
└──────────────────────────────────────────┴─────────────────────────────────┘
```

```sql
WITH
    '171.225.130.45' as IPv4_string
SELECT
    hex(IPv4StringToNum(IPv4_string)),
    hex(toIPv4(IPv4_string))
```

```plain%20text
┌─hex(IPv4StringToNum(IPv4_string))─┬─hex(toIPv4(IPv4_string))─┐
│ ABE1822D                          │ ABE1822D                 │
└───────────────────────────────────┴──────────────────────────┘
```

## toIPv6
Converts a string form of IPv6 address to IPv6 type. If the IPv6 address has an invalid format, returns an empty value.
Similar to IPv6StringToNum function, which converts IPv6 address to binary format.
<!-- TODO: CNCH does not support convert IPv4 to IPv6 so far
If the input string contains a valid IPv4 address, then the IPv6 equivalent of the IPv4 address is returned. -->

**Syntax**

```sql
toIPv6(string)
```

**Argument**
- `string` — IP address. String

**Returned value**
- IP address. Type: IPv6.

**Examples**

```sql
WITH '2001:438:ffff::407d:1bc1' AS IPv6_string
SELECT
    hex(IPv6StringToNum(IPv6_string)),
    hex(toIPv6(IPv6_string));
```

```plain%20text
┌─hex(IPv6StringToNum(IPv6_string))─┬─hex(toIPv6(IPv6_string))─────────┐
│ 20010438FFFF000000000000407D1BC1  │ 20010438FFFF000000000000407D1BC1 │
└───────────────────────────────────┴──────────────────────────────────┘
```
<!-- TODO: ByConity not work as expected

```sql
SELECT toIPv6('127.0.0.1');
```

```plain%20text
┌─toIPv6('127.0.0.1')─┐
│ ::ffff:127.0.0.1    │
└─────────────────────┘
``` -->
