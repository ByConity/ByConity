#pragma once

#include <Storages/MergeTree/IMetastore.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/SimpleIncrement.h>

namespace DB
{

class MergeTreeMeta {

public:

    using MetaStorePtr = std::shared_ptr<IMetaStore>;
    using MutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
    using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
    using MutableDataPartsVector = std::vector<MutableDataPartPtr>;

    MergeTreeMeta(const String _path, const String metastore_name_);

    ~MergeTreeMeta();

    /// Directly load metadata from metastore and restore all data parts.
    std::pair<MutableDataPartsVector, PartNamesWithDisks> loadFromMetastore(const MergeTreeData & storage);

    /// insert new part into metastore
    void addPart(const MergeTreeData & storage, const DataPartPtr & part);

    /// remove part from metastore
    void dropPart(const MergeTreeData & storage, const DataPartPtr & part);

    /// add new wal file into metastore
    void addWAL(const MergeTreeData & storage, const String & wal_file, const DiskPtr & disk);

    /// remove wal file from metastore
    void removeWAL(const MergeTreeData & storage, const String & wal_file);

    /// get wal file information from metastore.
    PartNamesWithDisks getWriteAheadLogs(const MergeTreeData & storage);

    /// load projections.
    void loadProjections(const MergeTreeData & storage);

    // for metadata management use. if key is not provided, clear all metadata from metastore.
    void dropMetaData(const MergeTreeData & storage, const String & key = "");

    /// check if can load from metastore
    bool checkMetastoreStatus(const MergeTreeData & storage);

    /// set status of metastore
    void setMetastoreStatus(const MergeTreeData & storage);

    /// raw interfaces to intereact with metastore;
    IMetaStore::IteratorPtr getMetaInfo(const String & prefix = "");

    /// open metastore.
    void openMetastore();

    /// Close metastore
    void closeMetastore();

    /// Clean all metadata in metastore
    void cleanMetastore();

    /** ----------------------- COMPATIBLE CODE BEGIN-------------------------- */
    /*  compatible with old metastore. remove this later  */
    bool checkMetaReady();
    std::pair<MutableDataPartsVector, PartNamesWithDisks> loadPartFromMetastore(const MergeTreeData & storage);
    /*  -----------------------  COMPATIBLE CODE END -------------------------- */


private:

    String path;
    String metastore_name;

    Poco::Logger * log;
    MetaStorePtr metastore;
    std::atomic_bool closed {false};
    std::mutex meta_mutex;

    /// add projections to metastore when committing data part.
 //   void addProjection(const MergeTreeData & storage, const String & name, const DataPartPtr & proj_part);

    /// remove projections from metastore when dropping data part.
 //   void dropProjection(const MergeTreeData & storage, const String & name, const DataPartPtr & proj_part);
};

}
