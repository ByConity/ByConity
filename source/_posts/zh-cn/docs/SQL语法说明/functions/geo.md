---
title: "Geo"
slug: "geo"
hidden: false
createdAt: "2021-07-29T12:05:09.208Z"
updatedAt: "2021-09-23T04:01:15.690Z"
categories:
- Docs
- SQL_Syntax
tags:
- Docs
---
> Notice:
Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

## geohashDecode
Decodes any geohash-encoded string into longitude and latitude.

**Syntax**

```sql
geohashDecode(encoded_string)
```

**Arguments**
- `encoded_string` - geohash-encoded string. 

**Returned values**
- longitude and latitude.

**Example**

```sql
SELECT geohashDecode('ezs42d000000') as res;
```

```plain%20text
┌─res─────────────────────────────────────┐
│ (-5.603027176111937, 42.59399422444403) │
└─────────────────────────────────────────┘
```

## geohashEncode 
Encodes latitude and longitude as a geohash-string.

**Syntax**

```sql
geohashEncode(longitude, latitude, [precision])
```

**Arguments**
- longitude - longitude part of the coordinate you want to encode. Floating in range `[-180°, 180°]` 
- latitude - latitude part of the coordinate you want to encode. Floating in range `[-90°, 90°]` 
- precision - Optional, length of the resulting encoded string, defaults to `12` . Integer in range `[1, 12]` . Any value less than `1` or greater than `12` is silently converted to `12` . 

**Returned values**
- alphanumeric `String` of encoded coordinate (modified version of the base32-encoding alphabet is used). 

**Example**

```sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res;
```

```plain%20text
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## greatCircleDistance
Calculates the distance between two points on the Earth’s surface using [the great-circle formula](https://en.wikipedia.org/wiki/Great-circle_distance) .

**Syntax**

```sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Arguments**
- `lon1Deg` — Longitude of the first point in degrees. Range: `[-180°, 180°]` . 
- `lat1Deg` — Latitude of the first point in degrees. Range: `[-90°, 90°]` . 
- `lon2Deg` — Longitude of the second point in degrees. Range: `[-180°, 180°]` . 
- `lat2Deg` — Latitude of the second point in degrees. Range: `[-90°, 90°]` . 
Positive values correspond to North latitude and East longitude, and negative values correspond to South latitude and West longitude.

**Returned value**
- The distance between two points on the Earth’s surface, in meters. Generates an exception when the input parameter values fall outside of the range.

**Example**

```sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673);
```

```plain%20text
┌─greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)─┐
│ 1.4132374194975413e+07                                            │
└───────────────────────────────────────────────────────────────────┘
```

## pointInEllipses
Checks whether the point belongs to at least one of the ellipses.
Coordinates are geometric in the Cartesian coordinate system.

**Syntax**

```sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**Arguments**
- `x, y` — Coordinates of a point on the plane. 
- `xᵢ, yᵢ` — Coordinates of the center of the `i` -th ellipsis. 
- `aᵢ, bᵢ` — Axes of the `i` -th ellipsis in units of x, y coordinates. 
The input parameters must be `2+4⋅n` , where `n` is the number of ellipses.

**Returned values**
- `1` if the point is inside at least one of the ellipses; `0` if it is not.

**Example**

```sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

```plain%20text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│ 1                                               │
└─────────────────────────────────────────────────┘
```

## pointInPolygon
Checks whether the point belongs to the polygon on the plane.

**Syntax**

```sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**Arguments**
- `(x, y)` — Coordinates of a point on the plane. Data type — Tuple — A tuple of two numbers. 
- `[(a, b), (c, d) ...]` — Polygon vertices. Data type — Array. Each vertex is represented by a pair of coordinates `(a, b)` . Vertices should be specified in a clockwise or counterclockwise order. The minimum number of vertices is 3. The polygon must be constant. 
- The function also supports polygons with holes (cut out sections). In this case, add polygons that define the cut out sections using additional arguments of the function. The function does not support non-simply-connected polygons. 

**Returned values**
- `1` if the point is inside the polygon, `0` if it is not. If the point is on the polygon boundary, the function may return either 0 or 1.

**Example**

```sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

```plain%20text
┌─res─┐
│ 1   │
└─────┘
```
