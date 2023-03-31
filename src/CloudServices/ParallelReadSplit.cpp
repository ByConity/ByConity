#include "CloudServices/ParallelReadSplit.h"
#include <Protos/data_models.pb.h>

#include "IO/ReadHelpers.h"
#include "IO/VarInt.h"
#include "IO/WriteBufferFromString.h"
#include "IO/WriteHelpers.h"
#include "Protos/DataModelHelpers.h"
#include "Storages/Hive/HiveDataPart.h"

namespace DB
{

void ParallelReadSplits::serialize(WriteBuffer & out) const
{
    writeVarUInt(raw.size(), out);
    for (const auto & data : raw)
        writeStringBinary(data, out);
}

void ParallelReadSplits::deserialize(ReadBuffer & in)
{
    size_t new_size = 0;
    readVarUInt(new_size, in);
    raw.resize(new_size);
    for (auto & data : raw)
        readStringBinary(data, in);
}

String ParallelReadSplits::describe() const
{
    return fmt::format("split size {}", raw.size());
}

void ParallelReadSplits::add(const IParallelReadSplit & split)
{
    WriteBufferFromOwnString s;
    split.serialize(s);
    raw.push_back(s.str());
}

HiveParallelReadSplit::HiveParallelReadSplit(HiveDataPartCNCHPtr part_, size_t start_, size_t end_) : part(part_), start(start_), end(end_)
{
}

void HiveParallelReadSplit::serialize(WriteBuffer & out) const
{
    part->serialize(out);
    writeVarUInt(start, out);
    writeVarUInt(end, out);
}

void HiveParallelReadSplit::deserialize(ReadBuffer & in)
{
    if (!part)
        part = std::make_shared<HiveDataPart>();
    part->deserialize(in);
    readVarUInt(start, in);
    readVarUInt(end, in);
}

String HiveParallelReadSplit::describe() const
{
    return fmt::format("hive data part {} [{}, {}]", part->name, start, end);
}

}
