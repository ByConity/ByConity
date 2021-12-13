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
#include <Processors/Sources/RemoteSource.h>
#include <Common/ClickHouseRevision.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Interpreters/Context.h>

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
