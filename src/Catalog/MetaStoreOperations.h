#pragma once

#include <string>

namespace DB
{

namespace Catalog
{

enum class OperationType
{
    OPEN,
    PUT,
    GET,
    MULTIGET,
    WRITEBATCH,
    MULTIWRITE,
    UPDATE,
    DELETE,
    SCAN,
    CLEAN,
    MULTIDROP,
    MULTIPUTCAS
};

const std::string Operation(const OperationType & type);

}

}
