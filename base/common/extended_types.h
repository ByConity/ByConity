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

#pragma once

#include <type_traits>

#include <common/types.h>
#include <common/wide_integer.h>

using Int128 = wide::integer<128, signed>;
using UInt128 = wide::integer<128, unsigned>;
using Int256 = wide::integer<256, signed>;
using UInt256 = wide::integer<256, unsigned>;
using Int512 = wide::integer<512, signed>;
using UInt512 = wide::integer<512, unsigned>;

struct PairInt64
{
    /// This naming assumes little endian.
    UInt64 low;
    UInt64 high;

    PairInt64() = default;
    explicit PairInt64(const UInt64 low_, const UInt64 high_) : low(low_), high(high_) {}
    explicit PairInt64(const UInt64 rhs) : low(rhs), high() {}

    auto tuple() const { return std::tie(high, low); }

    bool inline operator== (const PairInt64 rhs) const { return tuple() == rhs.tuple(); }
    bool inline operator!= (const PairInt64 rhs) const { return tuple() != rhs.tuple(); }
    bool inline operator<  (const PairInt64 rhs) const { return tuple() < rhs.tuple(); }
    bool inline operator<= (const PairInt64 rhs) const { return tuple() <= rhs.tuple(); }
    bool inline operator>  (const PairInt64 rhs) const { return tuple() > rhs.tuple(); }
    bool inline operator>= (const PairInt64 rhs) const { return tuple() >= rhs.tuple(); }

    template <typename T> bool inline operator== (T rhs) const { return *this == PairInt64(rhs); }
    template <typename T> bool inline operator!= (T rhs) const { return *this != PairInt64(rhs); }
    template <typename T> bool inline operator>= (T rhs) const { return *this >= PairInt64(rhs); }
    template <typename T> bool inline operator>  (T rhs) const { return *this >  PairInt64(rhs); }
    template <typename T> bool inline operator<= (T rhs) const { return *this <= PairInt64(rhs); }
    template <typename T> bool inline operator<  (T rhs) const { return *this <  PairInt64(rhs); }

    template <typename T> explicit operator T() const { return static_cast<T>(low); }

    PairInt64 & operator= (const UInt64 rhs) { low = rhs; high = 0; return *this; }
};

static_assert(sizeof(Int256) == 32);
static_assert(sizeof(UInt256) == 32);

/// The standard library type traits, such as std::is_arithmetic, with one exception
/// (std::common_type), are "set in stone". Attempting to specialize them causes undefined behavior.
/// So instead of using the std type_traits, we use our own version which allows extension.
template <typename T>
struct is_signed
{
    static constexpr bool value = std::is_signed_v<T>;
};

template <> struct is_signed<Int128> { static constexpr bool value = true; };
template <> struct is_signed<Int256> { static constexpr bool value = true; };
template <> struct is_signed<Int512> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_signed_v = is_signed<T>::value;

template <typename T>
struct is_unsigned // NOLINT(readability-identifier-naming)
{
    static constexpr bool value = std::is_unsigned_v<T>;
};

template <> struct is_unsigned<UInt128> { static constexpr bool value = true; };
template <> struct is_unsigned<UInt256> { static constexpr bool value = true; };
template <> struct is_unsigned<UInt512> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_unsigned_v = is_unsigned<T>::value;


/// TODO: is_integral includes char, char8_t and wchar_t.
template <typename T>
struct is_integer
{
    static constexpr bool value = std::is_integral_v<T>;
};

template <> struct is_integer<Int128> { static constexpr bool value = true; };
template <> struct is_integer<UInt128> { static constexpr bool value = true; };
template <> struct is_integer<Int256> { static constexpr bool value = true; };
template <> struct is_integer<UInt256> { static constexpr bool value = true; };
template <> struct is_integer<Int512> { static constexpr bool value = true; };
template <> struct is_integer<UInt512> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_integer_v = is_integer<T>::value;

template <class T> concept is_integer_v2 =
    std::is_integral_v<T>
    || std::is_same_v<T, Int128>
    || std::is_same_v<T, UInt128>
    || std::is_same_v<T, Int256>
    || std::is_same_v<T, UInt256>;

template <class T> concept is_floating_point = std::is_floating_point_v<T>;

template <typename T>
struct is_arithmetic // NOLINT(readability-identifier-naming)
{
    static constexpr bool value = std::is_arithmetic_v<T>;
};

template <> struct is_arithmetic<Int128> { static constexpr bool value = true; };
template <> struct is_arithmetic<UInt128> { static constexpr bool value = true; };
template <> struct is_arithmetic<Int256> { static constexpr bool value = true; };
template <> struct is_arithmetic<UInt256> { static constexpr bool value = true; };
template <> struct is_arithmetic<Int512> { static constexpr bool value = true; };
template <> struct is_arithmetic<UInt512> { static constexpr bool value = true; };


template <typename T>
inline constexpr bool is_arithmetic_v = is_arithmetic<T>::value;

template <typename T>
struct make_unsigned // NOLINT(readability-identifier-naming)
{
    using type = std::make_unsigned_t<T>;
};

template <> struct make_unsigned<Int128> { using type = UInt128; };
template <> struct make_unsigned<UInt128> { using type = UInt128; };
template <> struct make_unsigned<Int256>  { using type = UInt256; };
template <> struct make_unsigned<UInt256> { using type = UInt256; };
template <> struct make_unsigned<Int512>  { using type = UInt512; };
template <> struct make_unsigned<UInt512> { using type = UInt512; };

template <typename T> using make_unsigned_t = typename make_unsigned<T>::type;

template <typename T>
struct make_signed // NOLINT(readability-identifier-naming)
{
    using type = std::make_signed_t<T>;
};

template <> struct make_signed<Int128>  { using type = Int128; };
template <> struct make_signed<UInt128> { using type = Int128; };
template <> struct make_signed<Int256>  { using type = Int256; };
template <> struct make_signed<UInt256> { using type = Int256; };
template <> struct make_signed<Int512>  { using type = Int512; };
template <> struct make_signed<UInt512> { using type = Int512; };

template <typename T> using make_signed_t = typename make_signed<T>::type;

template <typename T>
struct is_big_int // NOLINT(readability-identifier-naming)
{
    static constexpr bool value = false;
};

template <> struct is_big_int<Int128> { static constexpr bool value = true; };
template <> struct is_big_int<UInt128> { static constexpr bool value = true; };
template <> struct is_big_int<Int256> { static constexpr bool value = true; };
template <> struct is_big_int<UInt256> { static constexpr bool value = true; };
template <> struct is_big_int<Int512> { static constexpr bool value = true; };
template <> struct is_big_int<UInt512> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_big_int_v = is_big_int<T>::value;

template <typename T>
struct is_pair_int64
{
    static constexpr bool value = false;
};

template <> struct is_pair_int64<PairInt64> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_pair_int64_v = is_pair_int64<T>::value;

