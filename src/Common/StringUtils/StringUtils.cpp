/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma clang diagnostic ignored "-Wunknown-warning-option"
#pragma clang diagnostic ignored "-Wreserved-identifier"
#include "StringUtils.h"
#include <type_traits>
#include <initializer_list>
#include <cstddef>
#include <cstring>
#include <cstdint>

#pragma clang diagnostic push
#if __has_warning("-Wreserved-identifier")
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

namespace detail
{

bool startsWith(const std::string & s, const char * prefix, size_t prefix_size)
{
    return s.size() >= prefix_size && 0 == memcmp(s.data(), prefix, prefix_size);
}

bool endsWith(const std::string & s, const char * suffix, size_t suffix_size)
{
    return s.size() >= suffix_size && 0 == memcmp(s.data() + s.size() - suffix_size, suffix, suffix_size);
}

void parseSlowQuery(const std::string & query, size_t & pos)
{
    const std::string whitespace = " \t\n";
    pos = query.find_first_not_of(whitespace, pos);
    if (pos == std::string::npos || query[pos] != '/')
        return;

    pos++;

    size_t length = query.size();
    if (pos >= length || query[pos] != '*')
        return;

    while (pos < length)
    {
        if (pos + 1 < length && query[pos] == '*' && query[pos + 1] == '/')
        {
            pos += 2;
            break;
        }
        pos++;
    }

    // recursively parse the query
    parseSlowQuery(query, pos);
}

// Convert lowerCamelCase and PascalCase strings to lower_with_underscore.
void convertCamelToSnake(std::string & orig_str) {
    std::string str(1, tolower(orig_str[0]));

    // First place underscores between contiguous lower and upper case letters.
    // For example, `_LowerCamelCase` becomes `_Lower_Camel_Case`.
    for (auto it = orig_str.begin() + 1; it != orig_str.end(); ++it)
    {
        if (isupper(*it) && (*(it-1) != '_'))
        {
            if (islower(*(it-1)) || (it + 1 != orig_str.end() && islower(*(it+1))))
            {
                str += "_";
            }
        }
        str += *it;
    }

    // Then convert it to lower case.
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);

    orig_str = str;
}

}

namespace compatibility
{

namespace v1
{

inline std::size_t
unaligned_load(const char* p)
{
  std::size_t result;
  __builtin_memcpy(&result, p, sizeof(result));
  return result;
}

#if __SIZEOF_SIZE_T__ == 8
  // Loads n bytes, where 1 <= n < 8.
  inline std::size_t
  load_bytes(const char* p, int n)
  {
    std::size_t result = 0;
    --n;
    do
      result = (result << 8) + static_cast<unsigned char>(p[n]);
    while (--n >= 0);
    return result;
  }

  inline std::size_t
  shift_mix(std::size_t v)
  { return v ^ (v >> 47);}
#endif

#if __SIZEOF_SIZE_T__ == 4

  // Implementation of Murmur hash for 32-bit size_t.
  size_t
  _Hash_bytes(const void* ptr, size_t len, size_t seed)
  {
    const size_t m = 0x5bd1e995;
    size_t hash = seed ^ len;
    const char* buf = static_cast<const char*>(ptr);

    // Mix 4 bytes at a time into the hash.
    while(len >= 4)
      {
	size_t k = unaligned_load(buf);
	k *= m;
	k ^= k >> 24;
	k *= m;
	hash *= m;
	hash ^= k;
	buf += 4;
	len -= 4;
      }

    size_t k;
    // Handle the last few bytes of the input array.
    switch(len)
      {
      case 3:
	k = static_cast<unsigned char>(buf[2]);
	hash ^= k << 16;
	[[gnu::fallthrough]];
      case 2:
	k = static_cast<unsigned char>(buf[1]);
	hash ^= k << 8;
	[[gnu::fallthrough]];
      case 1:
	k = static_cast<unsigned char>(buf[0]);
	hash ^= k;
	hash *= m;
      };

    // Do a few final mixes of the hash.
    hash ^= hash >> 13;
    hash *= m;
    hash ^= hash >> 15;
    return hash;
  }

#elif __SIZEOF_SIZE_T__ == 8

  // Implementation of Murmur hash for 64-bit size_t.
  size_t
  _Hash_bytes(const void* ptr, size_t len, size_t seed)
  {
    static const size_t mul = ((static_cast<size_t>(0xc6a4a793UL)) << 32UL)
			      + static_cast<size_t>(0x5bd1e995UL);
    const char* const buf = static_cast<const char*>(ptr);

    // Remove the bytes not divisible by the sizeof(size_t).  This
    // allows the main loop to process the data as 64-bit integers.
    const size_t len_aligned = len & ~static_cast<size_t>(0x7);
    const char* const end = buf + len_aligned;
    size_t hash = seed ^ (len * mul);
    for (const char* p = buf; p != end; p += 8)
      {
	const size_t data = shift_mix(unaligned_load(p) * mul) * mul;
	hash ^= data;
	hash *= mul;
      }
    if ((len & 0x7) != 0)
      {
	const size_t data = load_bytes(end, len & 0x7);
	hash ^= data;
	hash *= mul;
      }
    hash = shift_mix(hash) * mul;
    hash = shift_mix(hash);
    return hash;
  }

#else

  // Dummy hash implementation for unusual sizeof(size_t).
  size_t
  _Hash_bytes(const void* ptr, size_t len, size_t seed)
  {
    size_t hash = seed;
    const char* cptr = reinterpret_cast<const char*>(ptr);
    for (; len; --len)
      hash = (hash * 131) + *cptr++;
    return hash;
  }

#endif /* __SIZEOF_SIZE_T__ */

static size_t
hashImpl(const void* __ptr, size_t __clength,
 size_t __seed = static_cast<size_t>(0xc70f6907UL))
{ return _Hash_bytes(__ptr, __clength, __seed); }

size_t hash(const std::string &s)
{
    return hashImpl(s.c_str(), s.length());
}

}

namespace v2
{

using namespace std;

// We use murmur2 when size_t is 32 bits, and cityhash64 when size_t
// is 64 bits.  This is because cityhash64 uses 64bit x 64bit
// multiplication, which can be very slow on 32-bit systems.
template <class _Size, size_t = sizeof(_Size)*__CHAR_BIT__>
struct v2_murmur2_or_cityhash;

template <class _Size>
struct v2_murmur2_or_cityhash<_Size, 32>
{
    inline _Size operator()(const void* __key, _Size __len)
         _LIBCPP_DISABLE_UBSAN_UNSIGNED_INTEGER_CHECK;
};

// murmur2
template <class _Size>
_Size
v2_murmur2_or_cityhash<_Size, 32>::operator()(const void* __key, _Size __len)
{
    const _Size __m = 0x5bd1e995;
    const _Size __r = 24;
    _Size __h = __len;
    const unsigned char* __data = static_cast<const unsigned char*>(__key);
    for (; __len >= 4; __data += 4, __len -= 4)
    {
        _Size __k = __loadword<_Size>(__data);
        __k *= __m;
        __k ^= __k >> __r;
        __k *= __m;
        __h *= __m;
        __h ^= __k;
    }
    switch (__len)
    {
    case 3:
        __h ^= __data[2] << 16;
        _LIBCPP_FALLTHROUGH();
    case 2:
        __h ^= __data[1] << 8;
        _LIBCPP_FALLTHROUGH();
    case 1:
        __h ^= __data[0];
        __h *= __m;
    }
    __h ^= __h >> 13;
    __h *= __m;
    __h ^= __h >> 15;
    return __h;
}

template <class _Size>
struct v2_murmur2_or_cityhash<_Size, 64>
{
    inline _Size operator()(const void* __key, _Size __len)  _LIBCPP_DISABLE_UBSAN_UNSIGNED_INTEGER_CHECK;

 private:
  // Some primes between 2^63 and 2^64.
  static const _Size __k0 = 0xc3a5c85c97cb3127ULL;
  static const _Size __k1 = 0xb492b66fbe98f273ULL;
  static const _Size __k2 = 0x9ae16a3b2f90404fULL;
  static const _Size __k3 = 0xc949d7c7509e6557ULL;

  static _Size __rotate(_Size __val, int __shift) {
    return __shift == 0 ? __val : ((__val >> __shift) | (__val << (64 - __shift)));
  }

  static _Size __rotate_by_at_least_1(_Size __val, int __shift) {
    return (__val >> __shift) | (__val << (64 - __shift));
  }

  static _Size __shift_mix(_Size __val) {
    return __val ^ (__val >> 47);
  }

  static _Size __hash_len_16(_Size __u, _Size __v)
     _LIBCPP_DISABLE_UBSAN_UNSIGNED_INTEGER_CHECK
  {
    const _Size __mul = 0x9ddfea08eb382d69ULL;
    _Size __a = (__u ^ __v) * __mul;
    __a ^= (__a >> 47);
    _Size __b = (__v ^ __a) * __mul;
    __b ^= (__b >> 47);
    __b *= __mul;
    return __b;
  }

  static _Size __hash_len_0_to_16(const char* __s, _Size __len)
     _LIBCPP_DISABLE_UBSAN_UNSIGNED_INTEGER_CHECK
  {
    if (__len > 8) {
      const _Size __a = __loadword<_Size>(__s);
      const _Size __b = __loadword<_Size>(__s + __len - 8);
      return __hash_len_16(__a, __rotate_by_at_least_1(__b + __len, __len)) ^ __b;
    }
    if (__len >= 4) {
      const uint32_t __a = __loadword<uint32_t>(__s);
      const uint32_t __b = __loadword<uint32_t>(__s + __len - 4);
      return __hash_len_16(__len + (__a << 3), __b);
    }
    if (__len > 0) {
      const unsigned char __a = __s[0];
      const unsigned char __b = __s[__len >> 1];
      const unsigned char __c = __s[__len - 1];
      const uint32_t __y = static_cast<uint32_t>(__a) +
                           (static_cast<uint32_t>(__b) << 8);
      const uint32_t __z = __len + (static_cast<uint32_t>(__c) << 2);
      return __shift_mix(__y * __k2 ^ __z * __k3) * __k2;
    }
    return __k2;
  }

  static _Size __hash_len_17_to_32(const char *__s, _Size __len)
     _LIBCPP_DISABLE_UBSAN_UNSIGNED_INTEGER_CHECK
  {
    const _Size __a = __loadword<_Size>(__s) * __k1;
    const _Size __b = __loadword<_Size>(__s + 8);
    const _Size __c = __loadword<_Size>(__s + __len - 8) * __k2;
    const _Size __d = __loadword<_Size>(__s + __len - 16) * __k0;
    return __hash_len_16(__rotate(__a - __b, 43) + __rotate(__c, 30) + __d,
                         __a + __rotate(__b ^ __k3, 20) - __c + __len);
  }

  // Return a 16-byte hash for 48 bytes.  Quick and dirty.
  // Callers do best to use "random-looking" values for a and b.
  static pair<_Size, _Size> __weak_hash_len_32_with_seeds(
      _Size __w, _Size __x, _Size __y, _Size __z, _Size __a, _Size __b)
        _LIBCPP_DISABLE_UBSAN_UNSIGNED_INTEGER_CHECK
  {
    __a += __w;
    __b = __rotate(__b + __a + __z, 21);
    const _Size __c = __a;
    __a += __x;
    __a += __y;
    __b += __rotate(__a, 44);
    return pair<_Size, _Size>(__a + __z, __b + __c);
  }

  // Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
  static pair<_Size, _Size> __weak_hash_len_32_with_seeds(
      const char* __s, _Size __a, _Size __b)
    _LIBCPP_DISABLE_UBSAN_UNSIGNED_INTEGER_CHECK
  {
    return __weak_hash_len_32_with_seeds(__loadword<_Size>(__s),
                                         __loadword<_Size>(__s + 8),
                                         __loadword<_Size>(__s + 16),
                                         __loadword<_Size>(__s + 24),
                                         __a,
                                         __b);
  }

  // Return an 8-byte hash for 33 to 64 bytes.
  static _Size __hash_len_33_to_64(const char *__s, size_t __len)
    _LIBCPP_DISABLE_UBSAN_UNSIGNED_INTEGER_CHECK
  {
    _Size __z = __loadword<_Size>(__s + 24);
    _Size __a = __loadword<_Size>(__s) +
                (__len + __loadword<_Size>(__s + __len - 16)) * __k0;
    _Size __b = __rotate(__a + __z, 52);
    _Size __c = __rotate(__a, 37);
    __a += __loadword<_Size>(__s + 8);
    __c += __rotate(__a, 7);
    __a += __loadword<_Size>(__s + 16);
    _Size __vf = __a + __z;
    _Size __vs = __b + __rotate(__a, 31) + __c;
    __a = __loadword<_Size>(__s + 16) + __loadword<_Size>(__s + __len - 32);
    __z += __loadword<_Size>(__s + __len - 8);
    __b = __rotate(__a + __z, 52);
    __c = __rotate(__a, 37);
    __a += __loadword<_Size>(__s + __len - 24);
    __c += __rotate(__a, 7);
    __a += __loadword<_Size>(__s + __len - 16);
    _Size __wf = __a + __z;
    _Size __ws = __b + __rotate(__a, 31) + __c;
    _Size __r = __shift_mix((__vf + __ws) * __k2 + (__wf + __vs) * __k0);
    return __shift_mix(__r * __k0 + __vs) * __k2;
  }
};

// cityhash64
template <class _Size>
_Size
v2_murmur2_or_cityhash<_Size, 64>::operator()(const void* __key, _Size __len)
{
  const char* __s = static_cast<const char*>(__key);
  if (__len <= 32) {
    if (__len <= 16) {
      return __hash_len_0_to_16(__s, __len);
    } else {
      return __hash_len_17_to_32(__s, __len);
    }
  } else if (__len <= 64) {
    return __hash_len_33_to_64(__s, __len);
  }

  // For strings over 64 bytes we hash the end first, and then as we
  // loop we keep 56 bytes of state: v, w, x, y, and z.
  _Size __x = __loadword<_Size>(__s + __len - 40);
  _Size __y = __loadword<_Size>(__s + __len - 16) +
              __loadword<_Size>(__s + __len - 56);
  _Size __z = __hash_len_16(__loadword<_Size>(__s + __len - 48) + __len,
                          __loadword<_Size>(__s + __len - 24));
  pair<_Size, _Size> __v = __weak_hash_len_32_with_seeds(__s + __len - 64, __len, __z);
  pair<_Size, _Size> __w = __weak_hash_len_32_with_seeds(__s + __len - 32, __y + __k1, __x);
  __x = __x * __k1 + __loadword<_Size>(__s);

  // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
  __len = (__len - 1) & ~static_cast<_Size>(63);
  do {
    __x = __rotate(__x + __y + __v.first + __loadword<_Size>(__s + 8), 37) * __k1;
    __y = __rotate(__y + __v.second + __loadword<_Size>(__s + 48), 42) * __k1;
    __x ^= __w.second;
    __y += __v.first + __loadword<_Size>(__s + 40);
    __z = __rotate(__z + __w.first, 33) * __k1;
    __v = __weak_hash_len_32_with_seeds(__s, __v.second * __k1, __x + __w.first);
    __w = __weak_hash_len_32_with_seeds(__s + 32, __z + __w.second,
                                        __y + __loadword<_Size>(__s + 16));
    _VSTD::swap(__z, __x);
    __s += 64;
    __len -= 64;
  } while (__len != 0);
  return __hash_len_16(
      __hash_len_16(__v.first, __w.first) + __shift_mix(__y) * __k1 + __z,
      __hash_len_16(__v.second, __w.second) + __x);
}

template<class _Ptr>
inline _LIBCPP_INLINE_VISIBILITY
size_t v2_do_string_hash(_Ptr __p, _Ptr __e)
{
    typedef typename iterator_traits<_Ptr>::value_type value_type;
    return v2_murmur2_or_cityhash<size_t>()(__p, (__e-__p)*sizeof(value_type));
}

size_t hash(const std::string &s)
{
    return v2_do_string_hash(s.data(), s.data()+s.size());
}

}

}

#pragma clang diagnostic pop
