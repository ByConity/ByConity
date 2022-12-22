#include <TSO/TSOOperations.h>

namespace DB
{

namespace TSO
{

const std::string Operation(const OperationType & type)
{
    switch (type)
    {
        case OperationType::OPEN :
        return "OPEN";
        case OperationType::PUT :
        return "PUT";
        case OperationType::GET :
        return "GET";
        case OperationType::CLEAN :
        return "CLEAN";
    }

    __builtin_unreachable();
}

}

}
