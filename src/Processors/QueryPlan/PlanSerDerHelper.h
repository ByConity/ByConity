#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Block.h>

namespace DB
{

class IQueryPlanStep;
using QueryPlanStepPtr = std::unique_ptr<IQueryPlanStep>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

#define SERIALIZE_ENUM(ITEM, BUF) writeBinary(UInt8(ITEM), BUF);

#define DESERIALIZE_ENUM(TYPE, ITEM, BUF) \
    TYPE ITEM; \
    { \
        UInt8 tmp; \
        readBinary(tmp, BUF); \
        ITEM = TYPE(tmp); \
    }

template<typename Type> 
void serializeEnum(const Type & item, WriteBuffer & buf)
{
    writeBinary(UInt8(item), buf);
}

template<typename Type>
void deserializeEnum(Type & item, ReadBuffer & buf)
{
    UInt8 tmp;
    readBinary(tmp, buf);
    item = Type(tmp);
}

void serializeStrings(const Strings & strings, WriteBuffer & buf);
Strings deserializeStrings(ReadBuffer & buf);

void serializeBlock(const Block & block, WriteBuffer & buf);
Block deserializeBlock(ReadBuffer & buf);

void serializePlanStep(const QueryPlanStepPtr & step, WriteBuffer & buf);
QueryPlanStepPtr deserializePlanStep(ReadBuffer & buf, ContextPtr context);


}
