#pragma once
#include <CloudServices/ICnchBGThread.h>
#include <Storages/IStorage_fwd.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>

namespace DB
{

/// BG task to make checkpoint from manifests. Mean to reduce versions of the storage.
// 
// + - - - - - - - - - - - - - - - - - - - - - - - - -+
// '                   Checkpoint 1                   '
// '                                                  '
// ' +----------+     +------------+     +----------+ '     +----------+
// ' | table_v1 | --> |  table_v2  | --> | table_v3 | ' --> | table_v4 |
// ' +----------+     +------------+     +----------+ '     +----------+
// '                                                  '
// + - - - - - - - - - - - - - - - - - - - - - - - - -+
//
// table versions and correspoding manifests could be removed after checkopint finished.

class TableVersion;
class MergeTreeMetaBase;
using TableVersionPtr = std::shared_ptr<TableVersion>;

class CnchManifestCheckpointThread : public ICnchBGThread
{

public:
    CnchManifestCheckpointThread(ContextPtr context_, const StorageID & id);

    // execute 'system checkpoint db.table' to make checkpoint manually for debug
    void executeManually();
private:

    void runImpl() override;

    UInt64 checkPointImpl(StoragePtr & istorage);

    ServerDataPartsWithDBM loadPartsWithDBMFromTableVersions(const std::vector<TableVersionPtr> & table_versions, const MergeTreeMetaBase & storage);

    void checkPointServerPartsWithDBM(const MergeTreeMetaBase & storage, ServerDataPartsVector & server_parts, DeleteBitmapMetaPtrVector & delete_bitmaps, const UInt64 checkpoint_version);

    std::mutex task_mutex;
};


}
