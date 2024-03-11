#include <WorkerTasks/ManipulationTask.h>
#include <CloudServices/CnchServerClient.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class StorageCloudMergeTree;
struct AsyncRefreshParams;

class StorageMaterializedViewRefreshTask : public ManipulationTask
{
public:
    StorageMaterializedViewRefreshTask(StorageCloudMergeTree & storage_, ManipulationTaskParams params_, StorageID mv_storage_id_, ContextPtr context_, CnchServerClientPtr client);
    void executeImpl() override;
    void refreshAsyncOnWorker(AsyncRefreshParam & mv_refresh_param, ContextMutablePtr insert_context);

private:
    StorageCloudMergeTree & storage;
    StorageID mv_storage_id;
    CnchServerClientPtr server_client;
};

}
