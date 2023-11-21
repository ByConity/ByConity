#pragma once

#include <string.h>
#include <algorithm>
#include <type_traits>

#include <common/types.h>


/** \brief Returns value `from` converted to type `To` while retaining bit representation.
  *    `To` and `From` must satisfy `CopyConstructible`.
  */
template <typename To, typename From>
std::decay_t<To> bit_cast(const From & from)
{
    To res {};
    memcpy(static_cast<void*>(&res), &from, std::min(sizeof(res), sizeof(from)));
    return res;
}

/** \brief Returns value `from` converted to type `To` while retaining bit representation.
  *    `To` and `From` must satisfy `CopyConstructible`.
  */
template <typename To, typename From>
std::decay_t<To> safe_bit_cast(const From & from)
{
    static_assert(sizeof(To) == sizeof(From), "bit cast on types of different width");
    return bit_cast<To, From>(from);
}

template <typename T>
struct NumBits
{
    static constexpr unsigned int kBitsPerByte = 8;
    static constexpr UInt8 value = static_cast<UInt8>(sizeof(T) * kBitsPerByte);
    static_assert(sizeof(T) * kBitsPerByte <= UINT8_MAX, "number of bits in this structure larger than max uint8_t");
};
