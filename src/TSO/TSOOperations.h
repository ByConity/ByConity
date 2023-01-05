#pragma once

#include <string>

namespace DB
{

namespace TSO
{

enum class OperationType
{
    OPEN,
    PUT,
    GET,
    CLEAN
};

const std::string Operation(const OperationType & type);

}

}
