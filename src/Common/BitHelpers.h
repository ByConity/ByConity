#pragma once

#include <cstddef>
#include <cstdint>
#include <cassert>
#include <type_traits>
#include <common/defines.h>


/** For zero argument, result is zero.
  * For arguments with most significand bit set, result is n.
  * For other arguments, returns value, rounded up to power of two.
  */
inline size_t roundUpToPowerOfTwoOrZero(size_t n)
{
    // if MSB is set, return n, to avoid return zero
    if (unlikely(n >= 0x8000000000000000ULL))
        return n;

    --n;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    ++n;

    return n;
}


template <typename T>
inline size_t getLeadingZeroBitsUnsafe(T x)
{
    assert(x != 0);

    if constexpr (sizeof(T) <= sizeof(unsigned int))
    {
        return __builtin_clz(x);
    }
    else if constexpr (sizeof(T) <= sizeof(unsigned long int))
    {
        return __builtin_clzl(x);
    }
    else
    {
        return __builtin_clzll(x);
    }
}


template <typename T>
inline size_t getLeadingZeroBits(T x)
{
    if (!x)
        return sizeof(x) * 8;

    return getLeadingZeroBitsUnsafe(x);
}

/** Returns log2 of number, rounded down.
  * Compiles to single 'bsr' instruction on x86.
  * For zero argument, result is unspecified.
  */
template <typename T>
inline uint32_t bitScanReverse(T x)
{
    return (std::max<size_t>(sizeof(T), sizeof(unsigned int))) * 8 - 1 - getLeadingZeroBitsUnsafe(x);
}

// Unsafe since __builtin_ctz()-family explicitly state that result is undefined on x == 0
template <typename T>
inline size_t getTrailingZeroBitsUnsafe(T x)
{
    assert(x != 0);

    if constexpr (sizeof(T) <= sizeof(unsigned int))
    {
        return __builtin_ctz(x);
    }
    else if constexpr (sizeof(T) <= sizeof(unsigned long int))
    {
        return __builtin_ctzl(x);
    }
    else
    {
        return __builtin_ctzll(x);
    }
}

template <typename T>
inline size_t getTrailingZeroBits(T x)
{
    if (!x)
        return sizeof(x) * 8;

    return getTrailingZeroBitsUnsafe(x);
}

/** Returns a mask that has '1' for `bits` LSB set:
  * maskLowBits<UInt8>(3) => 00000111
  */
template <typename T>
inline T maskLowBits(unsigned char bits)
{
    if (bits == 0)
    {
        return 0;
    }

    T result = static_cast<T>(~T{0});
    if (bits < sizeof(T) * 8)
    {
        result = static_cast<T>(result >> (sizeof(T) * 8 - bits));
    }

    return result;
}

template <typename E>
constexpr auto to_underlying(E e) noexcept
{
    return static_cast<std::underlying_type_t<E>>(e);
}

#define bf_shf(x) (__builtin_ffsll(x) - 1)
#define GENMASK(h, l) (((~0UL) - (1UL << (l)) + 1) & (~0UL >> (sizeof(long)*8 - 1 - (h))))
#define FIELD_GET(_mask, _reg) static_cast<std::decay_t<decltype(_mask)>>(((_reg) & (_mask)) >> bf_shf(_mask))
#define FIELD_PREP(_mask, _val) (static_cast<std::decay_t<decltype(_mask)>>(_val) << bf_shf(_mask)) & (_mask)

#ifndef BIT
#define BIT(x) (1UL << static_cast<unsigned int>(x))
#endif

template <typename T>
constexpr bool isPowerOf2(T number)
{
    return number > 0 && (number & (number - 1)) == 0;
}
