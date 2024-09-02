#pragma once

#include <memory>
#include <utility>
#include <vector>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <CloudServices/ICnchBGThread.h>
#include <Core/Names.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Transaction/TxnTimestamp.h>

#include <Disks/HDFS/DiskByteHDFS.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <pcg_random.hpp>
#include <Poco/Logger.h>
#include "common/logger_useful.h"
#include <common/types.h>

namespace DB
{
class StorageCnchMergeTree;
class CnchBGThreadPartitionSelector;
using PartitionSelectorPtr = std::shared_ptr<CnchBGThreadPartitionSelector>;

// TODO(jiashuo.apache): wait support runImpl()
class CnchPartMoverThread : public ICnchBGThread
{
public:
    CnchPartMoverThread(ContextPtr context_, const StorageID & storage_id_)
        : ICnchBGThread(context_, CnchBGThreadType::PartMover, storage_id_)
    {
    }

    void runImpl() override { LOG_DEBUG(log, "Start run bytecool part move worker"); }
    void stop() override { ICnchBGThread::stop(); }
};
}
