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

    writeBinary(shuffle_keys.size(), buf);
    for (auto & key : shuffle_keys)
        writeBinary(key, buf);
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

    size_t key_size;
    readBinary(key_size, buf);
    shuffle_keys.resize(key_size);
    for (size_t i = 0; i < key_size; ++i)
        readBinary(shuffle_keys[i], buf);
}

String IPlanSegment::toString(size_t indent) const
{
    std::ostringstream ostr;
    String indent_str(indent, ' ');

    ostr << indent_str << "segment_id: " << segment_id << "\n";
    ostr << indent_str << "name: " << name << "\n";
    ostr << indent_str << "header: " << header.dumpStructure() << "\n";
    ostr << indent_str << "type: " << planSegmentTypeToString(type) << "\n";
    ostr << indent_str << "exchange_mode: " << exchangeModeToString(exchange_mode) << "\n";
    ostr << indent_str << "exchange_parallel_size: " << exchange_parallel_size << "\n";
    ostr << indent_str << "shuffle_keys: " << "\n";
    ostr << indent_str;
    for (auto & key : shuffle_keys)
        ostr << key << ", ";

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

String PlanSegmentInput::toString(size_t indent) const
{
    std::ostringstream ostr;
    String indent_str(indent, ' ');

    ostr << IPlanSegment::toString(indent) << "\n";
    ostr << indent_str << "parallel_index: " << parallel_index << "\n";
    ostr << indent_str << "source_addresses: " << "\n";
    ostr << indent_str;
    for (auto & address : source_addresses)
        ostr << address.toString() << "\n";

    return ostr.str();
}

void PlanSegmentOutput::serialize(WriteBuffer & buf) const
{
    IPlanSegment::serialize(buf);
    writeBinary(shuffle_function_name, buf);
    writeBinary(parallel_size, buf);
    writeBinary(keep_order, buf);
}

void PlanSegmentOutput::deserialize(ReadBuffer & buf)
{   
    IPlanSegment::deserialize(buf);
    readBinary(shuffle_function_name, buf);
    readBinary(parallel_size, buf);
    readBinary(keep_order, buf);
}

String PlanSegmentOutput::toString(size_t indent) const
{
    std::ostringstream ostr;
    String indent_str(indent, ' ');

    ostr << IPlanSegment::toString(indent) << "\n";
    ostr << indent_str << "shuffle_function_name: " << shuffle_function_name << "\n";
    ostr << indent_str << "parallel_size: " << parallel_size << "\n";
    ostr << indent_str << "keep_order: " << keep_order;

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
        ostr << input->toString(4) << "\n";
    ostr << "output: " << "\n";
    if (output)
        ostr << output->toString(4) << "\n";
    
    ostr << "coordinator_address: " << coordinator_address.toString() << "\n";
    ostr << "current_address: " << current_address.toString() << "\n";
    ostr << "cluster_name: " << cluster_name;

    return ostr.str(); 
}

String PlanSegmentTree::toString() const
{
    std::ostringstream ostr;

    std::queue<Node *> print_queue;
    print_queue.push(root);

    while (!print_queue.empty())
    {
        auto current = print_queue.front();
        print_queue.pop();

        for (auto & child : current->children)
            print_queue.push(child);

        ostr << current->plan_segment->toString() << "\n";
        ostr << " ------------------ " << "\n";
    }

    return ostr.str();
}

}
