#include <climits>
#include <utility>
#include <Functions/dtsCityHash.h>
#include <iostream>

namespace DB::DTSCityHash
{

static const Int64 k0 = 0xc3a5c85c97cb3127L;
static const Int64 k1 = 0xb492b66fbe98f273L;
static const Int64 k2 = 0x9ae16a3b2f90404fL;
static const Int64 k3 = 0xc949d7c7509e6557L;

UInt64 asUInt64(Int64 l) {
    if (l < 0) {
        return UINT64_MAX - static_cast<UInt64>(std::llabs(l)) + 1ULL;
    }
    return l;
}

static Int64 unsignedShift(Int64 val, int shift)
{
    return static_cast<UInt64>(val) >> shift;
}

static Int64 toLongLE(const char* b, int i) {
    return ((static_cast<Int64>(b[i + 7]) << 56) +
        (static_cast<Int64>(b[i + 6] & 255) << 48) +
        (static_cast<Int64>(b[i + 5] & 255) << 40) +
        (static_cast<Int64>(b[i + 4] & 255) << 32) +
        (static_cast<Int64>(b[i + 3] & 255) << 24) +
        ((b[i + 2] & 255) << 16) +
        ((b[i + 1] & 255) << 8) +
        ((b[i + 0] & 255) << 0));
}

static Int32 toIntLE(const char* b, int i) {
    return (((b[i + 3] & 255) << 24) + ((b[i + 2] & 255) << 16) + ((b[i + 1] & 255) << 8)
        + ((b[i + 0] & 255) << 0));
}

static Int64 fetch64(const char* s, int pos) {
    return toLongLE(s, pos);
}

static Int32 fetch32(const char* s, int pos) {
    return toIntLE(s, pos);
}

static Int64 rotate(Int64 val, int shift) {
    return shift == 0 ? val : unsignedShift(val, shift) | (val << (64 - shift));
}

static Int64 rotateByAtLeast1(Int64 val, int shift) {
    return unsignedShift(val, shift) | (val << (64 - shift));
}

static Int64 shiftMix(Int64 val) {
    return val ^ unsignedShift(val, 47);
}

static const Int64 kMul = 0x9ddfea08eb382d69LL;
static Int64 hash128to64(Int64 u, Int64 v) {
    Int64 a = (u ^ v) * kMul;
    a ^= unsignedShift(a, 47);
    Int64 b = (v ^ a) * kMul;
    b ^= unsignedShift(b, 47);
    b *= kMul;
    return b;
}

static Int64 hashLen16(Int64 u, Int64 v) {
    return hash128to64(u, v);
}

static Int64 hashLen0to16(const char* s, int pos, int len) {
    if (len > 8) {
      Int64 a = fetch64(s, pos + 0);
      Int64 b = fetch64(s, pos + len - 8);
      return hashLen16(a, rotateByAtLeast1(b + len, len)) ^ b;
    }
    if (len >= 4) {
      Int64 a = 0xffffffffL & fetch32(s, pos + 0);
      return hashLen16((a << 3) + len, 0xffffffffL & fetch32(s, pos + len - 4));
    }
    if (len > 0) {
      int a = s[pos + 0] & 0xFF;
      int b = s[pos + unsignedShift(len, 1)] & 0xFF;
      int c = s[pos + len - 1] & 0xFF;
      int y = a + (b << 8);
      int z = len + (c << 2);
      Int64 r = shiftMix(y * k2 ^ z * k3) * k2;
      return r;
    }
    return k2;
}

static Int64 hashLen17to32(const char* s, int pos, int len) {
    Int64 a = fetch64(s, pos + 0) * k1;
    Int64 b = fetch64(s, pos + 8);
    Int64 c = fetch64(s, pos + len - 8) * k2;
    Int64 d = fetch64(s, pos + len - 16) * k0;
    return hashLen16(
        rotate(a - b, 43) + rotate(c, 30) + d,
        a + rotate(b ^ k3, 20) - c + len
    );
}

static std::pair<Int64, Int64> weakHashLen32WithSeeds(
    Int64 w, Int64 x, Int64 y, Int64 z, Int64 a, Int64 b) {
    a += w;
    b = rotate(b + a + z, 21);
    Int64 c = a;
    a += x;
    a += y;
    b += rotate(a, 44);
    return std::make_pair(a + z, b + c );
}

static std::pair<Int64, Int64> weakHashLen32WithSeeds(const char* s, int pos, Int64 a, Int64 b) {
    return weakHashLen32WithSeeds(
        fetch64(s, pos + 0),
        fetch64(s, pos + 8),
        fetch64(s, pos + 16),
        fetch64(s, pos + 24),
        a,
        b
    );
}

static Int64 hashLen33to64(const char* s, int pos, int len) {
    Int64 z = fetch64(s, pos + 24);
    Int64 a = fetch64(s, pos + 0) + (fetch64(s, pos + len - 16) + len) * k0;
    Int64 b = rotate(a + z, 52);
    Int64 c = rotate(a, 37);

    a += fetch64(s, pos + 8);
    c += rotate(a, 7);
    a += fetch64(s, pos + 16);

    Int64 vf = a + z;
    Int64 vs = b + rotate(a, 31) + c;

    a = fetch64(s, pos + 16) + fetch64(s, pos + len - 32);
    z = fetch64(s, pos + len - 8);
    b = rotate(a + z, 52);
    c = rotate(a, 37);
    a += fetch64(s, pos + len - 24);
    c += rotate(a, 7);
    a += fetch64(s, pos + len - 16);

    Int64 wf = a + z;
    Int64 ws = b + rotate(a, 31) + c;
    Int64 r = shiftMix((vf + ws) * k2 + (wf + vs) * k0);

    return shiftMix(r * k0 + vs) * k2;
}

Int64 cityHash64(const char* s, int pos, int len)
{
    if (len <= 32) {
      if (len <= 16) {
        return hashLen0to16(s, pos, len);
      } else {
        return hashLen17to32(s, pos, len);
      }
    } else if (len <= 64) {
      return hashLen33to64(s, pos, len);
    }

    Int64 x = fetch64(s, pos + len - 40);
    Int64 y = fetch64(s, pos + len - 16) + fetch64(s, pos + len - 56);
    Int64 z = hashLen16(fetch64(s, pos + len - 48) + len, fetch64(s, pos + len - 24));

    auto v = weakHashLen32WithSeeds(s, pos + len - 64, len, z);
    auto w = weakHashLen32WithSeeds(s, pos + len - 32, y + k1, x);
    x = x * k1 + fetch64(s, pos + 0);

    len = (len - 1) & (~63);
    do {
      x = rotate(x + y + v.first + fetch64(s, pos + 8), 37) * k1;
      y = rotate(y + v.second + fetch64(s, pos + 48), 42) * k1;
      x ^= w.second;
      y += v.first + fetch64(s, pos + 40);
      z = rotate(z + w.first, 33) * k1;
      v = weakHashLen32WithSeeds(s, pos + 0, v.second * k1, x + w.first);
      w = weakHashLen32WithSeeds(s, pos + 32, z + w.second, y + fetch64(s, pos + 16));
      {
        Int64 swap = z;
        z = x;
        x = swap;
      }
      pos += 64;
      len -= 64;
    } while (len != 0);

    return hashLen16(
        hashLen16(v.first, w.first) + shiftMix(y) * k1 + z,
        hashLen16(v.second, w.second) + x
    );
}

}
