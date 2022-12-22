#include <Catalog/MetaStoreOperations.h>

namespace DB
{

namespace Catalog
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
        case OperationType::DELETE :
            return "DELETE";
        case OperationType::MULTIGET :
            return "MULTIGET";
        case OperationType::WRITEBATCH :
            return "WRITEBATCH";
        case OperationType ::MULTIWRITE:
            return "MULTIWRITE";
        case OperationType::UPDATE :
            return "UPDATE";
        case OperationType::SCAN :
            return "SCAN";
        case OperationType::CLEAN :
            return "CLEAN";
        case OperationType ::MULTIDROP :
            return "MULTIDROP";
        case OperationType ::MULTIPUTCAS:
            return "MULTIPUTCAS";
    }

    __builtin_unreachable();
}

}

}
