#include <Processors/QueryPlan/PlanSerDerHelper.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/ExchangeStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/FinishSortingStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergeSortingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/MergingSortedStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/PartialSortingStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/SettingQuotaAndLimitsStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/RemoteExchangeSourceStep.h>
#include <Common/ClickHouseRevision.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeHelper.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/JoinedTables.h>

namespace DB
{

void serializeStrings(const Strings & strings, WriteBuffer & buf)
{
    writeBinary(strings.size(), buf);
    for (auto & s : strings)
        writeBinary(s, buf);
}

Strings deserializeStrings(ReadBuffer & buf)
{
    size_t s_size;
    readBinary(s_size, buf);

    Strings strings(s_size);
    for (size_t i = 0; i < s_size; ++i)
        readBinary(strings[i], buf);

    return strings;
}

void serializeStringSet(const NameSet & stringSet, WriteBuffer & buf)
{
    writeBinary(stringSet.size(), buf);
    for (const auto& str: stringSet)
        writeBinary(str, buf);
}

NameSet deserializeStringSet(ReadBuffer & buf)
{
    size_t s_size;
    readBinary(s_size, buf);

    NameSet stringSet;
    for (size_t i = 0; i < s_size; ++i)
    {
        String str;
        readBinary(str, buf);
        stringSet.emplace(str);
    }

    return stringSet;
}

void serializeColumn(const ColumnPtr & column, const DataTypePtr & data_type, WriteBuffer & buf)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */

    writeBinary(isColumnConst(*column), buf);

    ColumnPtr full_column = column->convertToFullColumnIfConst();

    serializeDataType(data_type, buf);
    writeBinary(full_column->size(), buf);

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&buf](ISerialization::SubstreamPath) -> WriteBuffer * { return &buf; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0; //-V1048

    auto serialization = data_type->getDefaultSerialization();

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*full_column, 0, 0, settings, state);
    serialization->serializeBinaryBulkStateSuffix(settings, state);
}

ColumnPtr deserializeColumn(ReadBuffer & buf)
{
    bool is_const_column;
    readBinary(is_const_column, buf);

    auto data_type = deserializeDataType(buf);
    size_t rows;
    readBinary(rows, buf);

    ColumnPtr column = data_type->createColumn();

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &buf; };
    settings.avg_value_size_hint = 0;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    ISerialization::DeserializeBinaryBulkStatePtr state;
    auto serialization = data_type->getDefaultSerialization();

    serialization->deserializeBinaryBulkStatePrefix(settings, state);
    serialization->deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);

    if (column->size() != rows)
        throw Exception("Cannot read all data when deserialize column. Rows read: " + toString(column->size()) + ". Rows expected: " + toString(rows) + ".",
            ErrorCodes::CANNOT_READ_ALL_DATA);

    if (is_const_column)
        column = ColumnConst::create(column, rows);

    return column;
}

void serializeBlock(const Block & block, WriteBuffer & buf)
{
    BlockOutputStreamPtr block_out = std::make_shared<NativeBlockOutputStream>(
                                        buf,
                                        ClickHouseRevision::getVersionRevision(),
                                        block.cloneEmpty());
    block_out->write(block);
}

Block deserializeBlock(ReadBuffer & buf)
{
    BlockInputStreamPtr block_in = std::make_shared<NativeBlockInputStream>(
                                    buf,
                                    ClickHouseRevision::getVersionRevision());
    return block_in->read();
}

void serializeDataStream(const DataStream & data_stream, WriteBuffer & buf)
{
    serializeBlock(data_stream.header, buf);
    serializeStringSet(data_stream.distinct_columns, buf);
    writeBinary(data_stream.has_single_port, buf);
    serializeItemVector<SortColumnDescription>(data_stream.sort_description, buf);
    writeBinary(UInt8(data_stream.sort_mode), buf);
}

DataStream deserializeDataStream(ReadBuffer & buf)
{
    Block header;
    header = deserializeBlock(buf);

    NameSet distinct_columns = {};
    distinct_columns = deserializeStringSet(buf);

    bool has_single_port = false;
    readBinary(has_single_port, buf);

    SortDescription sort_description;
    sort_description = deserializeItemVector<SortColumnDescription>(buf);

    UInt8 sort_mode;
    readBinary(sort_mode, buf);

    return DataStream{.header = std::move(header),
                      .distinct_columns = std::move(distinct_columns),
                      .has_single_port = has_single_port,
                      .sort_description = std::move(sort_description),
                      .sort_mode = DataStream::SortMode(sort_mode)};
}

void serializeDataStreamFromDataStreams(const DataStreams & data_streams, WriteBuffer & buf)
{
    DataStream stream{.header = Block()};
    if (!data_streams.empty())
        stream = data_streams.front();

    serializeDataStream(stream, buf);
}

void serializeAggregatingTransformParams(const AggregatingTransformParamsPtr & params, WriteBuffer & buf)
{
    if (!params)
        throw Exception("Params cannot be nullptr", ErrorCodes::LOGICAL_ERROR);

    params->params.serialize(buf);
    writeBinary(params->final, buf);
}

AggregatingTransformParamsPtr deserializeAggregatingTransformParams(ReadBuffer & buf, ContextPtr context)
{
    Aggregator::Params params = Aggregator::Params::deserialize(buf, context);

    bool final;
    readBinary(final, buf);

    return std::make_shared<AggregatingTransformParams>(params, final);
}

void serializeArrayJoinAction(const ArrayJoinActionPtr & array_join, WriteBuffer & buf)
{
    if (!array_join)
    {
        writeBinary(false, buf);
        return;
    }

    writeBinary(true, buf);
    serializeStringSet(array_join->columns, buf);
    writeBinary(array_join->is_left, buf);
}

ArrayJoinActionPtr deserializeArrayJoinAction(ReadBuffer & buf, ContextPtr context)
{
    bool has_array_join = false;
    readBinary(has_array_join, buf);
    if (!has_array_join)
        return nullptr;

    NameSet columns = deserializeStringSet(buf);

    bool is_left;
    readBinary(is_left, buf);

    return std::make_shared<ArrayJoinAction>(columns, is_left, context);
}

void serializeTableJoin(const TableJoin & table_join, WriteBuffer & buf)
{
    ASTPtr select_query = table_join.getSelectQuery();
    select_query->serialize(buf);
}

TableJoinPtr deserializeTableJoin(ReadBuffer & buf, ContextPtr context)
{
    ASTPtr select_query = ASTSelectQuery::deserialize(buf);
    if (const auto * query = select_query->as<ASTSelectQuery>())
    {
        JoinedTables joined_tables(context, *query);
        return joined_tables.makeTableJoin(*query);
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "deserializeTableJoin needs ASTSelectQuery");
}

QueryPlanStepPtr deserializePlanStep(ReadBuffer & buf, ContextPtr context)
{
    IQueryPlanStep::Type type;
    {
        UInt8 tmp;
        readBinary(tmp, buf);
        type = IQueryPlanStep::Type(tmp);
    }

    switch (type)
    {
#define DESERIALIZE_STEP(TYPE) \
    case IQueryPlanStep::Type::TYPE: \
        return TYPE##Step::deserialize(buf, context);

        APPLY_STEP_TYPES(DESERIALIZE_STEP)

#undef DESERIALIZE_STEP
        default:
            break;
    }

    return nullptr;
}

void serializePlanStep(const QueryPlanStepPtr & step, WriteBuffer & buf)
{
    auto num = UInt8(step->getType());
    writeBinary(num, buf);
    step->serialize(buf);
}


}
