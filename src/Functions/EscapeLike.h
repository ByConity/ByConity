#pragma once
#include "EscapeMatchImpl.h"

namespace DB
{
struct NameEscapeILike
{
    static constexpr auto name = "escapeILike";
};

struct NameEscapeLike

{
    static constexpr auto name = "escapeLike";
};

struct NameEscapeRLike
{
    static constexpr auto name = "escapeRLike";
};

struct NameEscapeNotLike
{
    static constexpr auto name = "escapeNotLike";
};

struct NameEscapeNotILike
{
    static constexpr auto name = "escapeNotILike";
};

using EscapeLikeImpl = EscapeMatchImpl<NameEscapeLike, /*SQL LIKE */ true, /*revert*/false>;
using EscapeILikeImpl = EscapeMatchImpl<NameEscapeILike, true, false, /*case-insensitive*/true>;
using EscapeRLikeImpl = EscapeMatchImpl<NameEscapeRLike, /*SQL LIKE */ false, /*revert*/false>;
using EscapeNotLike = EscapeMatchImpl<NameEscapeNotLike, true, true>;
using EscapeNotILikeImpl = EscapeMatchImpl<NameEscapeNotILike, true, true, /*case-insensitive*/true>;

template <typename Impl, typename... Ts>
inline constexpr bool is_same_v = std::disjunction_v<std::is_same<Impl, Ts>...>;

template <typename Impl>
constexpr bool isEscapeMatchImpl()
{
    return is_same_v<Impl, EscapeLikeImpl, EscapeILikeImpl, EscapeRLikeImpl, EscapeNotLike, EscapeNotILikeImpl>;
}

};
