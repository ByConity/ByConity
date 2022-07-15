#include <WorkerTasks/ManipulationTask.h>

#include <Interpreters/Context_fwd.h>

namespace DB
{

class StorageCloudMergeTree;

class CloudMergeTreeMergeTask : public ManipulationTask
{
public:
    CloudMergeTreeMergeTask(StorageCloudMergeTree & storage_, ManipulationTaskParams params_, ContextPtr context_);
    void executeImpl() override;

private:
    StorageCloudMergeTree & storage;
};

}
