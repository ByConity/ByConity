#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Transforms/TableFinishTransform.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Transaction/ICnchTransaction.h>
#include <Poco/Logger.h>
#include "Common/StackTrace.h"
#include <common/logger_useful.h>
#include "Interpreters/ActionsVisitor.h"
#include "Interpreters/ProcessList.h"

namespace ProfileEvents
{
extern const Event InsertedRows;
extern const Event InsertedBytes;
}

namespace DB
{

TableFinishTransform::TableFinishTransform(
    const Block & header_, const StoragePtr & storage_, const ContextPtr & context_, ASTPtr & query_, bool insert_select_with_profiles_)
    : IProcessor({header_}, {header_})
    , input(inputs.front())
    , output(outputs.front())
    , storage(storage_)
    , context(context_)
    , query(query_)
    , insert_select_with_profiles(insert_select_with_profiles_)
{
}

void TableFinishTransform::setProcessListElement(QueryStatus * elem)
{
    process_list_elem = elem;
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

    current_output_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void TableFinishTransform::work()
{
    consume(std::move(current_output_chunk));
    has_input = false;
}

void TableFinishTransform::consume(Chunk chunk)
{
    output_chunk = std::move(chunk);

    if (insert_select_with_profiles && !output_chunk.empty())
    {
        auto & column = output_chunk.getColumns()[0];

        ReadProgress local_progress(column->get64(0), 0);

        ProfileEvents::increment(ProfileEvents::InsertedRows, local_progress.read_rows);
        ProfileEvents::increment(ProfileEvents::InsertedBytes, local_progress.read_bytes);

        if (process_list_elem)
        {
            process_list_elem->updateProgressOut(Progress(local_progress));
        }

        if (progress_callback)
        {
            progress_callback(Progress(local_progress));
        }
    }

    has_output = true;
}

void TableFinishTransform::onFinish()
{
    TransactionCnchPtr txn = context->getCurrentTransaction();
    txn->setMainTableUUID(storage->getStorageUUID());

    if (const auto * cnch_table = dynamic_cast<const StorageCnchMergeTree *>(storage.get());
        cnch_table && cnch_table->commitTxnInWriteSuffixStage(txn->getDedupImplVersion(context), context))
    {
        /// for unique table, insert select|infile is committed from worker side
    }
    else
        txn->commitV2();

    /// Make sure locks are release after transaction commit
    if (!lock_holders.empty())
        lock_holders.clear();
    LOG_DEBUG(getLogger("TableFinishTransform"), "Finish insert select commit in table finish.");
    output.finish();
}

}
