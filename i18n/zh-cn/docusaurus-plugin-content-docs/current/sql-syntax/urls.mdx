---
title: "URLs"
slug: "urls"
hidden: false
createdAt: "2021-07-29T12:32:13.195Z"
updatedAt: "2021-09-23T06:42:00.233Z"
tags:
  - Docs
---

> Notice:
> Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

## URLHierarchy

Returns an array containing the URL, truncated at the end by the symbols /,? in the path and query-string. Consecutive separator characters are counted as one. The cut is made in the position after all the consecutive separator characters.

**Syntax**

```sql
URLHierarchy(URL)
```

**Arguments**

- `URL` — URL. Type: String.

**Returned values**

- an array containing the URL

**Example**

```sql
SELECT URLHierarchy('https://example.com/browse/CONV-6788');
```

```plain%20text
┌─URLHierarchy('https://example.com/browse/CONV-6788')────────────────────────────────────────────┐
│ ['https://example.com/', 'https://example.com/browse/', 'https://example.com/browse/CONV-6788'] │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## URLPathHierarchy

The same as above, but without the protocol and host in the result. The / element (root) is not included.

**Syntax**

```sql
URLPathHierarchy(URL)
```

**Arguments**

- `URL` — URL. Type: String.

**Returned values**

- an array containing the URL

**Example**

```sql
SELECT URLPathHierarchy('https://example.com/browse/CONV-6788');
```

```plain%20text
┌─URLPathHierarchy('https://example.com/browse/CONV-6788')─┐
│ ['/browse/', '/browse/CONV-6788']                        │
└──────────────────────────────────────────────────────────┘
```

## cutFragment

Removes the fragment identifier. The number sign is also removed.

**Syntax**

```sql
cutFragment(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- url without fragment

**Example**

```sql
SELECT cutFragment('http://example.com#fragment')
```

```plain%20text
┌─cutFragment('http://example.com#fragment')─┐
│ http://example.com                         │
└────────────────────────────────────────────┘
```

## cutQueryString

Removes query string. The question mark is also removed.

**Syntax**

```sql
cutQueryString(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- url without query

**Example**

```sql
SELECT cutQueryString('http://example.com/?page=1&lr=213')
```

```plain%20text
┌─cutQueryString('http://example.com/?page=1&lr=213')─┐
│ http://example.com/                                 │
└─────────────────────────────────────────────────────┘
```

## cutQueryStringAndFragment

Removes the query string and fragment identifier. The question mark and number sign are also removed.

**Syntax**

```sql
cutQueryStringAndFragment(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- url string without query string and fragment

**Example**

```sql
SELECT cutQueryStringAndFragment('http://example.com/?page=1&lr=213#fragment')
```

```plain%20text
┌─cutQueryStringAndFragment('http://example.com/?page=1&lr=213#fragment')─┐
│ http://example.com/                                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

## cutToFirstSignificantSubdomain

Returns the part of the domain that includes top-level subdomains up to the “first significant subdomain”.

**Syntax**

```sql
cutToFirstSignificantSubdomain(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- subdomains string

**Example**

```sql
SELECT cutToFirstSignificantSubdomain('https://www.example.com.cn/')
```

```plain%20text
┌─cutToFirstSignificantSubdomain('https://www.example.com.cn/')─┐
│ example.com.cn                                                │
└───────────────────────────────────────────────────────────────┘
```

:::danger
GATEWAY-CLIENT not work as expected

```sql
SELECT cutToFirstSignificantSubdomain('www.tr')
```

```plain%20text
┌─cutToFirstSignificantSubdomain('www.tr')─┐
│ tr                                       │
└──────────────────────────────────────────┘
```

:::

## cutURLParameter

Removes the ‘name’ URL parameter, if present. This function works under the assumption that the parameter name is encoded in the URL exactly the same way as in the passed argument.

**Syntax**

```sql
cutURLParameter(URL, name)
```

**Arguments**

- `URL` – url string
- `name` - parameter name

**Returned value**

- url string

**Example**

```sql
SELECT cutURLParameter('http://example.com/?page=1&lr=213','page')
```

```plain%20text
┌─cutURLParameter('http://example.com/?page=1&lr=213', 'page')─┐
│ http://example.com/?lr=213                                   │
└──────────────────────────────────────────────────────────────┘
```

## cutWWW

Removes no more than one ‘www.’ from the beginning of the URL’s domain, if present.

**Syntax**

```sql
cutWWW(URL)
```

**Arguments**

- `URL` – url string
- `name` - parameter name

**Returned value**

- url string

**Example**

```sql
SELECT cutWWW('http://www.example.com/?page=1&lr=213')
```

```plain%20text
┌─cutWWW('http://www.example.com/?page=1&lr=213')─┐
│ http://example.com/?page=1&lr=213               │
└─────────────────────────────────────────────────┘
```

## decodeURLComponent

Returns the decoded URL.

**Syntax**

```sql
decodeURLComponent(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- decoded url string

**Example**

```sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

```plain%20text
┌─DecodedURL─────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1; │
└────────────────────────────────────────┘
```

## domain

Extracts the hostname from a URL.

**Syntax**

```sql
domain(url)
```

**Arguments**

- `url` — URL. Type: String.
  The URL can be specified with or without a scheme. Examples:

```plain%20text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

For these examples, the `domain` function returns the following results:

```plain%20text
some.svn-hosting.com
some.svn-hosting.com
yandex.com
```

**Returned values**

- Host name. If ByConity can parse the input string as a URL.
- Empty string. If ByConity can’t parse the input string as a URL.
  Type: `String` .

**Example**

```sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk');
```

```plain%20text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

## domainWithoutWWW

Returns the domain and removes no more than one ‘www.’ from the beginning of it, if present.

**Syntax**

```sql
domainWithoutWWW(url)
```

**Arguments**

- `url` — URL. Type: String.

**Returned values**

- Host name. If ByConity can parse the input string as a URL.
- Empty string. If ByConity can’t parse the input string as a URL.
  Type: `String` .

**Example**

```sql
SELECT domainWithoutWWW('http://www.example.com#fragment');
```

```plain%20text
┌─domainWithoutWWW('http://www.example.com#fragment')─┐
│ example.com                                         │
└─────────────────────────────────────────────────────┘
```

## extractURLParameter

Returns the value of the ‘name’ parameter in the URL, if present. Otherwise, an empty string. If there are many parameters with this name, it returns the first occurrence. This function works under the assumption that the parameter name is encoded in the URL exactly the same way as in the passed argument.

**Syntax**

```sql
extractURLParameter(URL, name)
```

**Arguments**

- `URL` – url string
- `name` - parameter name

**Returned value**

- parameter value

**Example**

```sql
SELECT extractURLParameter('http://example.com/?page=1&lr=213','page')
```

```plain%20text
┌─extractURLParameter('http://example.com/?page=1&lr=213', 'page')─┐
│ 1                                                                │
└──────────────────────────────────────────────────────────────────┘
```

## extractURLParameterNames

Returns an array of name strings corresponding to the names of URL parameters. The values are not decoded in any way.

**Syntax**

```sql
extractURLParameterNames(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- a list of parameter names

**Example**

```sql
SELECT extractURLParameterNames('http://example.com/?page=1&lr=213')
```

```plain%20text
┌─extractURLParameterNames('http://example.com/?page=1&lr=213')─┐
│ ['page', 'lr']                                                │
└───────────────────────────────────────────────────────────────┘
```

## extractURLParameters

Returns an array of name=value strings corresponding to the URL parameters. The values are not decoded in any way.

**Syntax**

```sql
extractURLParameters(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- a list of parameters

**Example**

```sql
SELECT extractURLParameters('http://example.com/?page=1&lr=213')
```

```plain%20text
┌─extractURLParameters('http://example.com/?page=1&lr=213')─┐
│ ['page=1', 'lr=213']                                      │
└───────────────────────────────────────────────────────────┘
```

## firstSignificantSubdomain

Returns the “first significant subdomain”. This is a non-standard concept specific to Yandex.Metrica. The first significant subdomain is a second-level domain if it is ‘com’, ‘net’, ‘org’, or ‘co’. Otherwise, it is a third-level domain.

**Syntax**

```sql
firstSignificantSubdomain(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- first significant subdomain

**Example**

```sql
SELECT firstSignificantSubdomain('https://www.example.com.cn/')
```

```plain%20text
┌─firstSignificantSubdomain('https://www.example.com.cn/')─┐
│ example                                                  │
└──────────────────────────────────────────────────────────┘
```

:::danger
GATEWAY-CLIENT not work as expected

```sql
SELECT firstSignificantSubdomain('www.tr')
```

````plain%20text
┌─firstSignificantSubdomain('www.tr')─┐
│ tr                                  │
└─────────────────────────────────────┘
```
:::

## fragment

Returns the fragment identifier. fragment does not include the initial hash symbol.

**Syntax**

```sql
fragment(URL)
````

**Arguments**

- `URL` – url string

**Returned value**

- fragment string

**Example**

```sql
SELECT fragment('http://example.com#fragment')
```

```plain%20text
┌─fragment('http://example.com#fragment')─┐
│ fragment                                │
└─────────────────────────────────────────┘
```

## path

Returns the path. Example: `/top/news.html` The path does not include the query string.

**Syntax**

```sql
path(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- path string

**Example**

```sql
SELECT path('http://example.com/top/news.html')
```

```plain%20text
┌─path('http://example.com/top/news.html')─┐
│ /top/news.html                           │
└──────────────────────────────────────────┘
```

## protocol

Extracts the protocol from a URL.
Examples of typical returned values: http, https, ftp, mailto, tel, magnet…

**Syntax**

```sql
protocol(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- protocol string

**Example**

```sql
SELECT protocol('http://example.com')
```

```plain%20text
┌─protocol('http://example.com')─┐
│ http                           │
└────────────────────────────────┘
```

## queryString

Returns the query string. Example: page=1&lr=213. query-string does not include the initial question mark, as well as # and everything after #.

**Syntax**

```sql
queryString(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- query string

**Example**

```sql
SELECT queryString('http://example.com/?page=1&lr=213')
```

```plain%20text
┌─queryString('http://example.com/?page=1&lr=213')─┐
│ page=1&lr=213                                    │
└──────────────────────────────────────────────────┘
```

## queryStringAndFragment

Returns the query string and fragment identifier. Example: page=1#29390.

**Syntax**

```sql
queryStringAndFragment(URL)
```

**Arguments**

- `URL` – url string

**Returned value**

- query and fragment string

**Example**

```sql
SELECT queryStringAndFragment('http://example.com/?page=1&lr=213#fragment')
```

```plain%20text
┌─queryStringAndFragment('http://example.com/?page=1&lr=213#fragment')─┐
│ page=1&lr=213#fragment                                               │
└──────────────────────────────────────────────────────────────────────┘
```

## topLevelDomain

Extracts the the top-level domain from a URL.

**Syntax**

```sql
topLevelDomain(url)
```

**Arguments**

- `url` — URL. Type: String.
  The URL can be specified with or without a scheme. Examples:

```plain%20text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

**Returned values**

- Domain name. If ByConity can parse the input string as a URL.
- Empty string. If ByConity cannot parse the input string as a URL.
  Type: `String` .

**Example**

```sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk');
```

```plain%20text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```
