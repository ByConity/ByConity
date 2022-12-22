#pragma once

namespace DB::FDB
{
    
enum FDBError
{
#define CREATE_ENUM(name, number) \
    FDB_##name = number,
#define ERROR(name, number, description) \
    CREATE_ENUM(name, number)
#include <foundationdb/error_definitions.h>
#undef CREATE_ENUM
};

}
