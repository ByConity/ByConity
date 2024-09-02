#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Processors/Transforms/TableFinishTransform.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Transaction/ICnchTransaction.h>
#include <Poco/Logger.h>
#include <Parsers/ASTInsertQuery.h>
#include <common/logger_useful.h>

namespace DB
{

TableFinishTransform::TableFinishTransform(const Block & header_, const StoragePtr & storage_, 
    const ContextPtr & context_, ASTPtr & query_)
    : IProcessor({header_}, {header_}), input(inputs.front())
    , output(outputs.front())
    , storage(storage_)
    , context(context_)
    , query(query_)
{
}

Block TableFinishTransform::getHeader()
{
    return header;
}

TableFinishTransform::Status TableFinishTransform::prepare()
{
    /// Handle drop partition for insert overwrite
    if (query)
    {
        auto * insert_query = dynamic_cast<ASTInsertQuery *>(query.get());
        auto * cnch_merge_tree = dynamic_cast<StorageCnchMergeTree*>(storage.get());
        if (insert_query && insert_query->is_overwrite && cnch_merge_tree)
        {
            cnch_merge_tree->overwritePartitions(insert_query->overwrite_partition, context, &lock_holders);
            query.reset();
        }
    }

    if (output.isFinished())
    {
        output.finish();
        if (has_output && output_chunk)
        {
            output.push(std::move(output_chunk));
            has_output = false;
        }

        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;

        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;

    if (input.isFinished())
    {
        onFinish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void TableFinishTransform::work()
{
    consume(std::move(current_chunk));
    has_input = false;
}

void TableFinishTransform::consume(Chunk chunk)
{
    output_chunk = std::move(chunk);
    has_output = true;
}

void TableFinishTransform::onFinish()
{
    TransactionCnchPtr txn = context->getCurrentTransaction();
    txn->setMainTableUUID(storage->getStorageUUID());
    txn->commitV2();

    /// Make sure locks are release after transaction commit
    if (!lock_holders.empty())
        lock_holders.clear();
    LOG_DEBUG(&Poco::Logger::get("TableFinishTransform"), "Finish insert select commit in table finish.");
    output.finish();
}

}
