#include <WorkerTasks/ManipulationTask.h>

#include <Interpreters/Context_fwd.h>

namespace DB
{

class StorageCloudMergeTree;

class CloudMergeTreeReclusterTask : public ManipulationTask
{
public:
    CloudMergeTreeReclusterTask(StorageCloudMergeTree & storage_, ManipulationTaskParams params_, ContextPtr context_);
    void executeImpl() override;

private:
    StorageCloudMergeTree & storage;
};

}
