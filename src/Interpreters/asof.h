#pragma once
#include <string>
#include <Protos/EnumMacros.h>
#include <Protos/enum.pb.h>

namespace DB
{
namespace ASOF
{

    ENUM_WITH_PROTO_CONVERTER(
        Inequality, // enum name
        Protos::ASOF::Inequality, // proto enum message
        (None, 0),
        (Less),
        (Greater),
        (LessOrEquals),
        (GreaterOrEquals));

    inline Inequality getInequality(const std::string & func_name)
    {
        Inequality inequality{Inequality::None};
        if (func_name == "less")
            inequality = Inequality::Less;
        else if (func_name == "greater")
            inequality = Inequality::Greater;
        else if (func_name == "lessOrEquals")
            inequality = Inequality::LessOrEquals;
        else if (func_name == "greaterOrEquals")
            inequality = Inequality::GreaterOrEquals;
        return inequality;
    }

inline Inequality reverseInequality(Inequality inequality)
{
    if (inequality == Inequality::Less)
        return Inequality::Greater;
    else if (inequality == Inequality::Greater)
        return Inequality::Less;
    else if (inequality == Inequality::LessOrEquals)
        return Inequality::GreaterOrEquals;
    else if (inequality == Inequality::GreaterOrEquals)
        return Inequality::LessOrEquals;
    return Inequality::None;
}

}
}
