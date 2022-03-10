SELECT murmurHash2_32(123456);
SELECT murmurHash2_32(CAST(3 AS UInt8));
SELECT murmurHash2_32(CAST(1.2684 AS Float32));
SELECT murmurHash2_32(CAST(-154477 AS Int64));
SELECT murmurHash2_32('foo');
SELECT murmurHash2_32(CAST('bar' AS FixedString(3)));
SELECT murmurHash2_32(x) FROM (SELECT CAST(1 AS Enum8('a' = 1, 'b' = 2)) as x);

SELECT murmurHash2_32();
SELECT murmurHash2_32('');
SELECT murmurHash2_32('\x01');
SELECT murmurHash2_32('\x02\0');
SELECT murmurHash2_32('\x03\0\0');
SELECT murmurHash2_32(1);
SELECT murmurHash2_32(toUInt16(2));

SELECT murmurHash2_32(2) = bitXor(toUInt32(0x5bd1e995 * bitXor(toUInt32(3 * 0x5bd1e995) AS a, bitShiftRight(a, 13))) AS b, bitShiftRight(b, 15));
SELECT murmurHash2_32('\x02') = bitXor(toUInt32(0x5bd1e995 * bitXor(toUInt32(3 * 0x5bd1e995) AS a, bitShiftRight(a, 13))) AS b, bitShiftRight(b, 15));

SELECT murmurHash2_64('foo');
SELECT murmurHash2_64('\x01');
SELECT murmurHash2_64(1);

SELECT murmurHash3_32('foo');
SELECT murmurHash3_32('\x01');
SELECT murmurHash3_32(1);

SELECT murmurHash3_64('foo');
SELECT murmurHash3_64('\x01');
SELECT murmurHash3_64(1);

SELECT gccMurmurHash('foo');
SELECT gccMurmurHash('\x01');
SELECT gccMurmurHash(1);

SELECT hex(murmurHash3_128('foo'));
SELECT hex(murmurHash3_128('\x01'));

SELECT murmurHash2_32WithSeed(123456, 0);
SELECT murmurHash2_32WithSeed(CAST(3 AS UInt8), 0);
SELECT murmurHash2_32WithSeed(CAST(1.2684 AS Float32), 0);
SELECT murmurHash2_32WithSeed(CAST(-154477 AS Int64), 0);
SELECT murmurHash2_32WithSeed('foo', 0);
SELECT murmurHash2_32WithSeed(CAST('bar' AS FixedString(3)), 0);
SELECT murmurHash2_32WithSeed(x, 0) FROM (SELECT CAST(1 AS Enum8('a' = 1, 'b' = 2)) as x);

SELECT murmurHash2_32WithSeed();  -- { serverError 42 }
SELECT murmurHash2_32WithSeed(0);
SELECT murmurHash2_32WithSeed('', 0);
SELECT murmurHash2_32WithSeed('\x01', 0);
SELECT murmurHash2_32WithSeed('\x02\0', 0);
SELECT murmurHash2_32WithSeed('\x03\0\0', 0);
SELECT murmurHash2_32WithSeed(1, 0);
SELECT murmurHash2_32WithSeed(toUInt16(2), 0);

SELECT murmurHash2_64WithSeed('foo', 0);
SELECT murmurHash2_64WithSeed('\x01', 0);
SELECT murmurHash2_64WithSeed(1, 0);

SELECT murmurHash3_32WithSeed('foo', 0);
SELECT murmurHash3_32WithSeed('\x01', 0);
SELECT murmurHash3_32WithSeed(1, 0);

SELECT murmurHash3_64WithSeed('foo', 0);
SELECT murmurHash3_64WithSeed('\x01', 0);
SELECT murmurHash3_64WithSeed(1, 0);

SELECT hex(murmurHash3_128WithSeed('foo', 0));
SELECT hex(murmurHash3_128WithSeed('\x01', 0));

SELECT murmurHash2_32WithSeed(123456, 727);
SELECT murmurHash2_32WithSeed(CAST(3 AS UInt8), 727);
SELECT murmurHash2_32WithSeed(CAST(1.2684 AS Float32), 727);
SELECT murmurHash2_32WithSeed(CAST(-154477 AS Int64), 727);
SELECT murmurHash2_32WithSeed('foo', 727);
SELECT murmurHash2_32WithSeed(CAST('bar' AS FixedString(3)), 727);
SELECT murmurHash2_32WithSeed(x, 727) FROM (SELECT CAST(1 AS Enum8('a' = 1, 'b' = 2)) as x);

SELECT murmurHash2_32WithSeed(); -- { serverError 42 }
SELECT murmurHash2_32WithSeed(727);
SELECT murmurHash2_32WithSeed('', 727);
SELECT murmurHash2_32WithSeed('\x01', 727);
SELECT murmurHash2_32WithSeed('\x02\0', 727);
SELECT murmurHash2_32WithSeed('\x03\0\0', 727);
SELECT murmurHash2_32WithSeed(1, 727);
SELECT murmurHash2_32WithSeed(toUInt16(2), 727);

SELECT murmurHash2_64WithSeed('foo', 727);
SELECT murmurHash2_64WithSeed('\x01', 727);
SELECT murmurHash2_64WithSeed(1, 727);

SELECT murmurHash3_32WithSeed('foo', 727);
SELECT murmurHash3_32WithSeed('\x01', 727);
SELECT murmurHash3_32WithSeed(1, 727);

SELECT murmurHash3_64WithSeed('foo', 727);
SELECT murmurHash3_64WithSeed('\x01', 727);
SELECT murmurHash3_64WithSeed(1, 727);

SELECT hex(murmurHash3_128WithSeed('foo', 727));
SELECT hex(murmurHash3_128WithSeed('\x01', 727));
