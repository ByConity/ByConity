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

void serializeStrings(const Strings & strings, WriteBuffer & buf);
Strings deserializeStrings(ReadBuffer & buf);

void serializeBlock(const Block & block, WriteBuffer & buf);
Block deserializeBlock(ReadBuffer & buf);

void serializePlanStep(const QueryPlanStepPtr & step, WriteBuffer & buf);
QueryPlanStepPtr deserializePlanStep(ReadBuffer & buf, ContextPtr context);


}
