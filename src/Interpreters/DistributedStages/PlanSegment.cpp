#include <Interpreters/DistributedStages/PlanSegment.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Core/ColumnNumbers.h>
#include <Parsers/IAST.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/PlanSerDerHelper.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String planSegmentTypeToString(const PlanSegmentType & type)
{
    std::ostringstream ostr;

    switch(type)
    {
        case PlanSegmentType::UNKNOWN:
            ostr << "UNKNOWN";
            break;
        case PlanSegmentType::SOURCE:
            ostr << "SOURCE";
            break;
        case PlanSegmentType::EXCHANGE:
            ostr << "EXCHANGE";
            break;
        case PlanSegmentType::OUTPUT:
            ostr << "OUTPUT";
            break;
    }

    return ostr.str();
}

void IPlanSegment::serialize(WriteBuffer & buf) const
{
    serializeBlock(header, buf);
    writeBinary(UInt8(type), buf);
    writeBinary(UInt8(exchange_mode), buf);
    writeBinary(exchange_parallel_size, buf);
    writeBinary(name, buf);
    writeBinary(segment_id, buf);
}

void IPlanSegment::deserialize(ReadBuffer & buf)
{
    header = deserializeBlock(buf);

    UInt8 read_type;
    readBinary(read_type, buf);
    type = PlanSegmentType(read_type);

    UInt8 read_mode;
    readBinary(read_mode, buf);
    exchange_mode = ExchangeMode(read_mode);

    readBinary(exchange_parallel_size, buf);
    readBinary(name, buf);
    readBinary(segment_id, buf);
}

String IPlanSegment::toString() const
{
    std::ostringstream ostr;

    ostr << "segment_id: " << segment_id << "\n";
    ostr << "name: " << name << "\n";
    ostr << "header: " << header.dumpStructure() << "\n";
    ostr << "type: " << planSegmentTypeToString(type) << "\n";
    ostr << "exchange_mode: " << exchangeModeToString(exchange_mode) << "\n";
    ostr << "exchange_parallel_size: " << exchange_parallel_size << "\n";

    return ostr.str();
}

void PlanSegmentInput::serialize(WriteBuffer & buf) const
{
    IPlanSegment::serialize(buf);

    writeBinary(parallel_index, buf);

    writeBinary(source_addresses.size(), buf);
    for (auto & source_address : source_addresses)
        source_address.serialize(buf);
}

void PlanSegmentInput::deserialize(ReadBuffer & buf)
{
    IPlanSegment::deserialize(buf);

    readBinary(parallel_index, buf);

    size_t addresses_size;
    readBinary(addresses_size, buf);
    for (size_t i = 0; i < addresses_size; ++i)
    {
        AddressInfo address;
        address.deserialize(buf);
        source_addresses.push_back(address);
    }
}

String PlanSegmentInput::toString() const
{
    std::ostringstream ostr;

    ostr << IPlanSegment::toString() << "\n";
    ostr << "parallel_index: " << parallel_index << "\n";
    ostr << "source_addresses: " << "\n";
    for (auto & address : source_addresses)
        ostr << address.toString() << "\n";

    return ostr.str();
}

void PlanSegmentOutput::serialize(WriteBuffer & buf) const
{
    writeBinary(shuffle_keys.size(), buf);
    for (auto & key : shuffle_keys)
        writeBinary(key, buf);
    
    writeBinary(shuffle_function_name, buf);
    writeBinary(parallel_size, buf);
    writeBinary(keep_order, buf);
}

void PlanSegmentOutput::deserialize(ReadBuffer & buf)
{
    size_t key_size;
    readBinary(key_size, buf);
    shuffle_keys.resize(key_size);
    for (size_t i = 0; i < key_size; ++i)
        readBinary(shuffle_keys[i], buf);
    
    readBinary(shuffle_function_name, buf);
    readBinary(parallel_size, buf);
    readBinary(keep_order, buf);
}

String PlanSegmentOutput::toString() const
{
    std::ostringstream ostr;

    ostr << IPlanSegment::toString() << "\n";
    ostr << "shuffle_keys: " << "\n";
    for (auto & key : shuffle_keys)
        ostr << key << "\n";
    ostr << "shuffle_function_name: " << shuffle_function_name << "\n";
    ostr << "parallel_size: " << parallel_size << "\n";
    ostr << "keep_order: " << keep_order << "\n";

    return ostr.str();
}

void PlanSegment::serialize(WriteBuffer & buf) const
{
    writeBinary(segment_id, buf);
    writeBinary(query_id, buf);
    
    query_plan.serialize(buf);

    writeBinary(inputs.size(), buf);
    for (auto & input : inputs)
        input->serialize(buf);

    if (output)
        output->serialize(buf);
    else
        throw Exception("Cannot find output when serialize PlanSegment", ErrorCodes::LOGICAL_ERROR);
    
    coordinator_address.serialize(buf);
    current_address.serialize(buf);

    writeBinary(cluster_name, buf);
}

void PlanSegment::deserialize(ReadBuffer & buf)
{
    readBinary(segment_id, buf);
    readBinary(query_id, buf);

    query_plan.addInterpreterContext(context);
    query_plan.deserialize(buf);

    size_t input_size;
    readBinary(input_size, buf);
    for (size_t i = 0; i < input_size; ++i)
    {
        auto input = std::make_shared<PlanSegmentInput>();
        input->deserialize(buf);
        inputs.push_back(input);
    }

    output = std::make_shared<PlanSegmentOutput>();
    output->deserialize(buf);
    coordinator_address.deserialize(buf);
    current_address.deserialize(buf);

    readBinary(cluster_name, buf);
}

PlanSegmentPtr PlanSegment::deserializePlanSegment(ReadBuffer & buf, ContextMutablePtr context_)
{
    auto plan_segment = std::make_unique<PlanSegment>(context_);
    plan_segment->deserialize(buf);
    return plan_segment;
}

String PlanSegment::toString() const
{
    std::ostringstream ostr;

    ostr << "segment_id: " << segment_id << "\n";
    ostr << "query_id: " << query_id << "\n";

    WriteBufferFromOwnString plan_str;
    query_plan.explainPlan(plan_str, {});
    ostr << plan_str.str() << "\n";

    ostr << "inputs: " << "\n";
    for (auto & input : inputs)
        ostr << input->toString() << "\n";
    ostr << "output: " << "\n";
    if (output)
        ostr << output->toString() << "\n";
    
    ostr << "coordinator_address: " << coordinator_address.toString() << "\n";
    ostr << "current_address: " << current_address.toString() << "\n";
    ostr << "clustr_name: " << cluster_name;

    return ostr.str(); 
}

}
