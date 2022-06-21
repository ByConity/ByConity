#include <CloudServices/commitCnchParts.h>

#include <CloudServices/CnchMergeMutateThread.h>
#include <Interpreters/Context.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Transaction/Actions/DDLAlterAction.h>
#include <Transaction/Actions/DropRangeAction.h>
#include <Transaction/Actions/InsertAction.h>
#include <Transaction/Actions/MergeMutateAction.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BRPC_TIMEOUT;
    extern const int BAD_DATA_PART_NAME;
    extern const int NULL_POINTER_DEREFERENCE;
    extern const int BUCKET_TABLE_ENGINE_MISMATCH;
}

void commitPreparedCnchParts(const StoragePtr & storage, const Context & context, PreparedCnchParts & params, TxnTimestamp & commit_time)
{
    Stopwatch watch;

    auto type = params.type;

    if (context.getServerType() != ServerType::cnch_server)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Must be called in Server mode: {}", context.getServerType());

    auto * data = dynamic_cast<MergeTreeMetaBase *>(storage.get());
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected MergeTree class");

    auto * log = storage->getLogger();

    auto & txn_coordinator = context.getCnchTransactionCoordinator();
    TransactionCnchPtr txn; /// TODO:
    // auto txn = context.getCurrentTransaction();
    auto txn_id = txn->getTransactionID();

    /// set main table uuid in server side
    txn->setMainTableUUID(storage->getStorageUUID());

    if (!params.from_buffer_uuid.empty())
    {
        txn->setFromBufferUUID(params.from_buffer_uuid);
        LOG_DEBUG(log, "set buffer uuid and main table uuid: {} {}  done.", txn->getFromBufferUUID(), toString(txn->getMainTableUUID()));
    }

    do
    {
        if (type == ManipulationType::Insert)
        {
            if (params.prepared_parts.empty() && params.delete_bitmaps.empty() && params.prepared_staged_parts.empty()
                && params.consumer_group.empty())
            {
                LOG_DEBUG(log, "Nothing to commit, we skip this call.");
                break;
            }

            // if (!tpl.empty() && !consumer_group.empty())
            //     txn->setKafkaTpl(consumer_group, tpl);

            /// check the part is already correctly clustered for bucket table. All new inserted parts should be clustered.
            // if (storage->isBucketTable())
            // {
            //     auto table_definition_hash = storage->getTableHashForClusterBy();
            //     for (auto & part : prepared_parts)
            //     {
            //         if (context.getSettings().skip_table_definition_hash_check)
            //             part->table_definition_hash = table_definition_hash;

            //         if (!part->deleted &&
            //             (part->bucket_number < 0 || table_definition_hash != part->table_definition_hash))
            //         {
            //             throw Exception(
            //                 "Part " + part->name + " is not clustered or it has different table definition with storage. Part bucket number : "
            //                 + std::to_string(part->bucket_number) + ", part table_definition_hash : [" + std::to_string(part->table_definition_hash)
            //                 + "], table's table_definition_hash : [" + std::to_string(table_definition_hash) + "]",
            //                 ErrorCodes::BUCKET_TABLE_ENGINE_MISMATCH);
            //         }
            //     }
            // }

            // Precommit stage. Write intermediate parts to KV
            // auto action
            //     = txn->createAction<InsertAction>(storage, params.prepared_parts, params.delete_bitmaps, params.prepared_staged_parts);
            // auto action = txn->createAction<InsertAction>(context, );
            // txn->appendAction(action);
            // action->executeV2();
        }
        else if (type == ManipulationType::Drop)
        {
            if (params.prepared_parts.empty())
            {
                LOG_DEBUG(log, "No parts to commit, we skip this call.");
                break;
            }

            auto action = txn->createAction<DropRangeAction>(txn->getTransactionRecord(), storage);
            // for (auto & part : params.prepared_parts)
            //     action->appendPart(part);
            for (auto & bitmap : params.delete_bitmaps)
                action->appendDeleteBitmap(bitmap);

            txn->appendAction(std::move(action));
            commit_time = txn_coordinator.commitV2(txn);

            LOG_TRACE(
                log,
                "Committed {} parts in transaction {}, elapsed {} ms",
                params.prepared_parts.size(),
                txn_id.toUInt64(),
                watch.elapsedMilliseconds());
        }
        else if (
            type == ManipulationType::Merge || type == ManipulationType::Clustering
            || type == ManipulationType::Mutate)
        {
            auto bg_thread = context.getCnchBGThread(CnchBGThreadType::MergeMutate, storage->getStorageID());
            auto * merge_mutate_thread = dynamic_cast<CnchMergeMutateThread *>(bg_thread.get());

            if (params.prepared_parts.empty())
            {
                LOG_WARNING(log, "No parts to commit, worker may failed to merge parts, which task_id is {}", params.task_id);
                merge_mutate_thread->tryRemoveTask(params.task_id);
            }
            else
            {
                // merge_mutate_thread->finishTask(params.task_id, params.prepared_parts.front(), [&] {
                //     auto action = txn->createAction<MergeMutateAction>(txn->getTransactionRecord(), type, storage);

                //     // for (auto & part : params.prepared_parts)
                //     //     action->appendPart(part);

                //     action->setDeleteBitmaps(params.delete_bitmaps);
                //     txn->appendAction(std::move(action));
                //     commit_time = txn_coordinator.commitV2(txn);

                //     LOG_TRACE(
                //         log,
                //         "Committed {} parts in transaction {}, elapsed {} ms",
                //         params.prepared_parts.size(),
                //         txn_id.toUInt64(),
                //         watch.elapsedMilliseconds());
                // });
            }
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support commit type {}", typeToString(type));
        }
    } while (false);
}
}
