#pragma once
#include <Common/Exception.h>
#include <Parsers/formatTenantDatabaseName.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
    }
}

#define DISABLE_VISIT_FOR_TENANTS() \
do { \
    if (context->getIsRestrictSystemTables() && !getCurrentTenantId().empty()) \
    { \
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "It is currently disable to visit the system table: " \
            + getTableName() +" in cloud environment."); \
    } \
} while (0)
