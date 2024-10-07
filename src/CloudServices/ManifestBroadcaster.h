#pragma once
#include <Interpreters/Context_fwd.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <CloudServices/CnchWorkerClient.h>
#include <MergeTreeCommon/assignCnchParts.h>
#include <Protos/cnch_worker_rpc.pb.h>
#include <Common/Exception.h>

namespace DB
{

template<typename ManifestDataType>
std::unordered_map<String, std::vector<std::shared_ptr<ManifestDataType>>> assignData(
    const ContextPtr & context,
    const WorkerGroupHandle & worker_group,
    const MergeTreeMetaBase & storage,
    const std::vector<std::shared_ptr<ManifestDataType>> & data_vector)
{
    std::unordered_map<String, std::vector<std::shared_ptr<ManifestDataType>>> res;

    if (data_vector.empty())
        return res;

    if (storage.isBucketTable())
    {
        std::vector<std::shared_ptr<ManifestDataType>> data_without_bucket_number;
        auto worker_ids = worker_group->getWorkerIDVec();
        sort(worker_ids.begin(), worker_ids.end());
        auto worker_num = worker_ids.size();
        for (const auto & data_ptr : data_vector)
        {
            Int64 bucket_number = data_ptr->bucketNumber();
            if (bucket_number == -1)
                data_without_bucket_number.emplace_back(data_ptr);
            else
                res[worker_ids[(bucket_number % worker_num)]].emplace_back(data_ptr);
        }

        // assign parts without bucket number to all workers
        for (size_t i=0; i<worker_num; i++)
            res[worker_ids[i]].insert(res[worker_ids[i]].end(), data_without_bucket_number.begin(), data_without_bucket_number.end());
    }
    else
    {
        res =  assignCnchParts(worker_group, data_vector, context, storage.getSettings(), Context::PartAllocator::JUMP_CONSISTENT_HASH);
    }

    return res;
}


void tryBroadcastManifest(
    ContextPtr & session_context,
    const StoragePtr & table,
    const TxnTimestamp & txnID,
    const Protos::DataModelPartVector & parts_model,
    const DeleteBitmapMetaPtrVector & delete_bitmaps)
{
    static Poco::Logger * log = &Poco::Logger::get("broadcastManifest");

    try
    {
        auto * storage = dynamic_cast<MergeTreeMetaBase *>(table.get());
        if (!storage)
        {
            LOG_WARNING(log, "Cannot broadcast manifest for non-CnchMergeTree table {}.", storage->getLogName());
            return;
        }

        auto worker_group = getWorkerGroupForTable(*storage, session_context);
        session_context->setCurrentWorkerGroup(worker_group);

        if (worker_group)
        {
            /***
             * allocate data parts and delete bitmaps among workers
             * TODO: Do we need to assign the manifest data among workers? Since most write transaction only commits several parts/dbms at a time.
             * Once we spread the data among workers, we need to maintain the worker info in worker side and the cache will be invalid once 
             * the worker topology change.
            */
            // DataModelPartWrapperVector parts_vector;
            // for (const auto & part_model : parts_model.parts())
            //     parts_vector.emplace_back(createPartWrapperFromModel(*storage, std::move(part_model)));
            // auto part_assignment = assignData(session_context, worker_group, *storage, parts_vector);
            // auto dbm_assignment = assignData(session_context, worker_group, *storage, delete_bitmaps);

            std::vector<brpc::CallId> call_ids;
            const auto & host_ports_vec = worker_group->getHostWithPortsVec();
            ExceptionHandlerPtr exception_handler_ptr = std::make_shared<ExceptionHandler>();

            for (const auto & host_port : host_ports_vec)
            {
                auto worker_id = WorkerStatusManager::getWorkerId(worker_group->getVWName(), worker_group->getID(), host_port.id);
                auto worker_client = worker_group->getWorkerClient(host_port);

                auto call_id = worker_client->broadcastManifest(
                    session_context,
                    txnID,
                    worker_id,
                    table,
                    parts_model,
                    delete_bitmaps,
                    exception_handler_ptr);

                call_ids.push_back(call_id);
            }

            for (auto & call_id : call_ids)
                brpc::Join(call_id);

            exception_handler_ptr->throwIfException();
        }
    }
    catch(...)
    {
        tryLogCurrentException(log, "Fail to broadcast manifest (table: " + table->getStorageID().getFullTableName() + ", manifest: " + txnID.toString() + ") to workers");
    }
}

}
