#pragma once

#include <CloudServices/ICnchBGThread.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <CloudServices/DedupWorkerStatus.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class StorageCnchMergeTree;

/**
 * Manage dedup worker of one unique table that enables staging area for writing.
 */
class DedupWorkerManager: public ICnchBGThread
{
public:

    DedupWorkerManager(ContextPtr context, const StorageID & storage_id);

    ~DedupWorkerManager() override;
    
    void runImpl() override;
    
    void stop() override;

    /**
     * Check whether target dedup worker instance is valid.
     */
    DedupWorkerHeartbeatResult reportHeartbeat(const String & worker_table_name);

    DedupWorkerStatus getDedupWorkerStatus();

private:
    
    void iterate(StoragePtr & istorage);
    
    void assignDeduperToWorker(StoragePtr & cnch_table);

    /// caller must hold lock of worker_client_mutex.
    void selectDedupWorker(StorageCnchMergeTree & cnch_table);

    void stopDeduperWorker();

    bool checkDedupWorkerStatus(StoragePtr & storage);

    String getDedupWorkerDebugInfo();

    mutable std::mutex worker_client_mutex;
    CnchWorkerClientPoolPtr worker_pool;
    CnchWorkerClientPtr worker_client;
    StorageID worker_storage_id;

};

}
