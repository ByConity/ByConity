#pragma once
#include <DataTypes/DataTypesNumber.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashMap.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

enum CompressEnum
{
    NO_COMPRESS_SORTED = 0,
    COMPRESS_BIT,
    NO_COMPRESS_NO_SORTED
};

template<CompressEnum compress>
struct compress_trait{};

using RType = UInt64;
struct AggregateFunctionRetentionData
{
    //    Array retentions;
    RType retentions[1];
};

}

