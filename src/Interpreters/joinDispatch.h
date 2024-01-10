#pragma once

#include <array>
#include <common/constexpr_helpers.h>

#include <Interpreters/HashJoin.h>


/** Used in implementation of Join to process different data structures.
  */

namespace DB
{

template <ASTTableJoin::Kind kind, typename ASTTableJoin::Strictness, bool has_ineuqal = false>
struct MapGetter;

///  When has_ineuqal = false all template instantiation is normal.
template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::RightAny>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::RightAny> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::RightAny> { using Map = HashJoin::MapsOne; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::RightAny>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = true; };

template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Any> { using Map = HashJoin::MapsOne; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::Any> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::Any>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };

template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::All>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::All> { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::All> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::All>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };

/// Only SEMI LEFT and SEMI RIGHT are valid. INNER and FULL are here for templates instantiation.
template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Semi>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Semi> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::Semi> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::Semi>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };

/// Only SEMI LEFT and SEMI RIGHT are valid. INNER and FULL are here for templates instantiation.
template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Anti>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Anti> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::Anti> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::Anti>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };

/// When has_ineuqal = true , only left anti and left semi are valid, others is for templates instantiation.
template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::RightAny, true>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::RightAny, true> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::RightAny, true> { using Map = HashJoin::MapsOne; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::RightAny, true>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = true; };

template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any, true>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Any, true> { using Map = HashJoin::MapsOne; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::Any, true> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::Any, true>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };

template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::All, true>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::All, true> { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::All, true> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::All, true>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };

template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Semi, true>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Semi, true> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::Semi, true> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::Semi, true>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };

template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Anti, true>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Anti, true> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::Anti, true> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::Anti, true>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };


template <ASTTableJoin::Kind kind>
struct MapGetter<kind, ASTTableJoin::Strictness::Asof> { using Map = HashJoin::MapsAsof; static constexpr bool flagged = false; };

template <ASTTableJoin::Kind kind>
struct MapGetter<kind, ASTTableJoin::Strictness::Asof, true> { using Map = HashJoin::MapsAsof; static constexpr bool flagged = false; };

/// Join strictness 
static constexpr std::array<ASTTableJoin::Kind, 4> KINDS = {
    ASTTableJoin::Kind::Left,
    ASTTableJoin::Kind::Inner,
    ASTTableJoin::Kind::Full,
    ASTTableJoin::Kind::Right
};

/// Join kind
static constexpr std::array<ASTTableJoin::Strictness, 6> STRICTNESSES = {
    ASTTableJoin::Strictness::RightAny,
    ASTTableJoin::Strictness::Any,
    ASTTableJoin::Strictness::All,
    ASTTableJoin::Strictness::Asof,
    ASTTableJoin::Strictness::Semi,
    ASTTableJoin::Strictness::Anti,
};

/// whether there is inequal expression in join conditions 
static constexpr std::array<bool, 2> INEQUALS = {
    false,
    true
};

/// Init specified join map
inline bool joinDispatchInit(ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness, HashJoin::MapsVariant & maps, bool has_ineuqal = false)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size() * INEQUALS.size()>([&](auto ijk)
    {
        constexpr auto k = ijk / (KINDS.size() * STRICTNESSES.size());
        constexpr auto j = (ijk / KINDS.size()) % STRICTNESSES.size();
        constexpr auto i = ijk % KINDS.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j] && has_ineuqal == INEQUALS[k])
        {
            maps = typename MapGetter<KINDS[i], STRICTNESSES[j], INEQUALS[k]>::Map();
            return true;
        }
        return false;
    });
}

/// Call function on specified join map
template <typename MapsVariant, typename Func>
inline bool joinDispatch(ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness, MapsVariant & maps, Func && func, bool has_ineuqal = false)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size() * INEQUALS.size()>([&](auto ijk)
    {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto k = ijk / (KINDS.size() * STRICTNESSES.size());
        constexpr auto j = (ijk / KINDS.size()) % STRICTNESSES.size();
        constexpr auto i = ijk % KINDS.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j] && has_ineuqal == INEQUALS[k])
        {
            func(
                std::integral_constant<ASTTableJoin::Kind, KINDS[i]>(),
                std::integral_constant<ASTTableJoin::Strictness, STRICTNESSES[j]>(),
                std::get<typename MapGetter<KINDS[i], STRICTNESSES[j], INEQUALS[k]>::Map>(maps));
            return true;
        }
        return false;
    });
}

}
