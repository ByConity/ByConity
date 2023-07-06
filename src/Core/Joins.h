#pragma once

#include <string_view>

namespace DB
{

/// Join method.
enum class JoinKind
{
    Inner, /// Leave only rows that was JOINed.
    Left, /// If in "right" table there is no corresponding rows, use default values instead.
    Right,
    Full,
    Cross, /// Direct product. Strictness and condition doesn't matter.
    Comma /// Same as direct product. Intended to be converted to INNER JOIN with conditions from WHERE.
};

const char * toString(JoinKind kind);

/// Algorithm for distributed query processing.
enum class JoinLocality
{
    Unspecified,
    Local, /// Perform JOIN, using only data available on same servers (co-located data).
    Global /// Collect and merge data from remote servers, and broadcast it to each server.
};

const char * toString(JoinLocality locality);

/// ASOF JOIN inequality type
enum class ASOFJoinInequality
{
    None,
    Less,
    Greater,
    LessOrEquals,
    GreaterOrEquals,
};

const char * toString(ASOFJoinInequality asof_join_inequality);

inline constexpr ASOFJoinInequality getASOFJoinInequality(std::string_view func_name)
{
    ASOFJoinInequality inequality = ASOFJoinInequality::None;

    if (func_name == "less")
        inequality = ASOFJoinInequality::Less;
    else if (func_name == "greater")
        inequality = ASOFJoinInequality::Greater;
    else if (func_name == "lessOrEquals")
        inequality = ASOFJoinInequality::LessOrEquals;
    else if (func_name == "greaterOrEquals")
        inequality = ASOFJoinInequality::GreaterOrEquals;

    return inequality;
}

inline constexpr ASOFJoinInequality reverseASOFJoinInequality(ASOFJoinInequality inequality)
{
    if (inequality == ASOFJoinInequality::Less)
        return ASOFJoinInequality::Greater;
    else if (inequality == ASOFJoinInequality::Greater)
        return ASOFJoinInequality::Less;
    else if (inequality == ASOFJoinInequality::LessOrEquals)
        return ASOFJoinInequality::GreaterOrEquals;
    else if (inequality == ASOFJoinInequality::GreaterOrEquals)
        return ASOFJoinInequality::LessOrEquals;

    return ASOFJoinInequality::None;
}

enum class JoinTableSide
{
    Left,
    Right
};

const char * toString(JoinTableSide join_table_side);

}
