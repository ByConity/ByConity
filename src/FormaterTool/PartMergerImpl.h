#pragma once

#include <Common/Logger.h>
#include <string>
#include <Disks/HDFS/DiskByteHDFS.h>
#include "Common/ErrorCodes.h"
#include "Common/tests/gtest_global_context.h"
#include "common/types.h"
#include <common/logger_useful.h>
#include "CloudServices/CnchCreateQueryHelper.h"
#include "CloudServices/CnchPartsHelper.h"
#include "Disks/DiskLocal.h"
#include "Disks/SingleDiskVolume.h"
#include "Disks/StoragePolicy.h"
#include "Disks/VolumeJBOD.h"
#include "FormaterTool/PartToolkitBase.h"
#include "Interpreters/Context.h"
#include "Protos/DataModelHelpers.h"
#include "Storages/IStorage.h"
#include "Storages/IStorage_fwd.h"
#include "Storages/MergeTree/IMergeTreeDataPart_fwd.h"
#include "Storages/MergeTree/MergeTreeCNCHDataDumper.h"
#include "Storages/MergeTree/MergeTreeDataMergerMutator.h"
#include "Storages/MergeTree/MergeTreeDataPartCNCH.h"
#include "Storages/StorageCloudMergeTree.h"
#include "Transaction/CnchServerTransaction.h"
#include "Transaction/TransactionCommon.h"
#include "WorkerTasks/CloudMergeTreeMergeTask.h"
#include "WorkerTasks/ManipulationTask.h"
#include "WorkerTasks/ManipulationTaskParams.h"
#include "WorkerTasks/MergeTreeDataMerger.h"

namespace DB
{

class MergeTreeMetaBase;
class CloudMergeTreeMergeTask;

using MergeTask = std::unique_ptr<CloudMergeTreeMergeTask>;
using MergeTasks = std::vector<MergeTask>;

using String = std::string;

struct PartMergerParams
{
    String create_table_query;
    String uuids_str;
    String source_path;
    String output_path;
    size_t concurrency = 1;
};

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int DIRECTORY_ALREADY_EXISTS;
}

/**
 * @class PartMergerImpl
 * @brief Impl merging logic.
 *
 */
class PartMergerImpl : public PartToolkitBase
{
    using MergeTreeMetaBaseSharedPtr = std::shared_ptr<MergeTreeMetaBase>;
    using StorageCloudMergeTreePtr = std::shared_ptr<StorageCloudMergeTree>;

public:
    PartMergerImpl(ContextMutablePtr context, Poco::Util::AbstractConfiguration & config, LoggerPtr log_);

    /**
     * Main entry for selecting and merging.
     */
    void execute() override;

private:
    void applySettings();

    /**
     * Collects parts from remote_path.
     */
    IMergeTreeDataPartsVector collectSourceParts(const std::vector<StorageCloudMergeTreePtr> & merge_trees);

    /**
     * Copies part data from a disk to another.
     */
    static void copyPartData(const DiskPtr & from_disk, const String & from_path, const DiskPtr & to_disk, const String & to_path);

    std::shared_ptr<StorageCloudMergeTree> createStorage(const String & path, const String & create_table_query);
    std::vector<std::shared_ptr<StorageCloudMergeTree>>
    createStorages(const std::vector<String> & uuids, const String & create_table_query);

    /**
     * Executes merge task.
     */
    void executeMergeTask(MergeTreeMetaBase & merge_tree, DiskPtr & disk, const MergeTask & task);

    PartMergerParams params;
    LoggerPtr log;
};


}
