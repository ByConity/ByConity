#include <Storages/MergeTree/CloudMergeTreeBlockOutputStream.h>

#include <Interpreters/PartLog.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <WorkerTasks/ManipulationType.h>
#include <CloudServices/commitCnchParts.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int CNCH_LOCK_ACQUIRE_FAILED;
    extern const int INSERTION_LABEL_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
    extern const int UNIQUE_KEY_STRING_SIZE_LIMIT_EXCEEDED;
}

Block CloudMergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}


void CloudMergeTreeBlockOutputStream::write(const Block & block)
{
    auto part_blocks
        = writer.splitBlockIntoParts(block, context->getSettingsRef().max_partitions_per_insert_block, metadata_snapshot, context);
    LOG_TRACE(storage.getLogger(), "size of part_blocks {} ", part_blocks.size());

    /// const auto & txn = context->getCurrentTransaction();
    auto part_log = context->getGlobalContext()->getPartLog(storage.getDatabaseName());
    MergeTreeMutableDataPartsVector temp_parts;
    auto txn_id = context->getCurrentTransactionID();
    auto block_id = context->getTimestamp();
    for (auto & block_with_partition : part_blocks)
    {
        Stopwatch watch;

        MergeTreeMutableDataPartPtr temp_part = writer.writeTempPart(block_with_partition, metadata_snapshot, context, block_id, txn_id);

        if (part_log)
            part_log->addNewPart(context, temp_part, watch.elapsed());
        LOG_TRACE(storage.getLogger(), "Wrote {}, elapsed {} ms", temp_part->name, watch.elapsedMilliseconds());

        temp_parts.push_back(std::move(temp_part));
    }
    CnchDataWriter cnch_writer(storage, *context, ManipulationType::Insert);
    auto dumped = cnch_writer.dumpAndCommitCnchParts(temp_parts);

    // batch all part to preload_parts for batch preloading in writeSuffix
    /// LOG_DEBUG(storage.getLogger(), "Pushing {} parts to preload vector.", temp_parts.size());
    /// std::move(temp_parts.begin(), temp_parts.end(), std::back_inserter(preload_parts));
}

void CloudMergeTreeBlockOutputStream::writeSuffix()
{
    try
    {
        writeSuffixImpl();
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::INSERTION_LABEL_ALREADY_EXISTS)
        {
            LOG_DEBUG(storage.getLogger(), e.displayText());
            return;
        }
        throw;
    }
}

void CloudMergeTreeBlockOutputStream::writeSuffixImpl()
{
    auto txn = context->getCurrentTransaction(); 
    if (dynamic_pointer_cast<CnchServerTransaction>(txn) && !disable_transaction_commit)
    {
        txn->setMainTableUUID(storage.getStorageUUID());
        txn->commitV2();
        LOG_DEBUG(storage.getLogger(), "Finishing insert values commit in cnch server.");
    }
    /// TODO: handling commit for worker side

    if (!preload_parts.empty())
    {
        /// auto testlog = std::make_shared<TestLog>(const_cast<Context &>(context));
        /// TEST_START(testlog);
        /// tryPreloadChecksumsAndPrimaryIndex(storage, std::move(preload_parts), ManipulationType::Insert, context);
        /// TEST_END(testlog, "Finish tryPreloadChecksumsAndPrimaryIndex in batch mode");
    }
}

}
