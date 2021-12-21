#pragma once

#include <Storages/MergeTree/BitEngineDictionary/IBitEngineDictionaryManager.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionarySnapshot.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDataExchanger.h>
#include <Storages/MergeTree/HaMergeTreeAddress.h>

#include <Interpreters/InterserverIOHandler.h>
#include <Common/RWLock.h>
#include <common/logger_useful.h>

// TODO (liuhaoqiang) remove it after all function finished
#pragma  GCC diagnostic ignored  "-Wunused"
#pragma GCC diagnostic ignored "-Wunused-parameter"

namespace DB
{
class MergeTreeData;
class StorageHaMergeTree;
class BitEngineDictionarySnapshot;
class BitEngineDataService;
class BitEngineDataExchanger;
class BitEngineDictionary;
class IncrementOffset;
class IncrementData;

using BitEngineDataExchangerPtr = std::shared_ptr<BitEngineDataExchanger>;
using BitEngineDictionaryPtr = std::shared_ptr<BitEngineDictionary>;

class BitEngineDictionaryManager final : public BitEngineDictionaryManagerBase<BitEngineDictionaryPtr>
{
public:
    BitEngineDictionaryManager(const String & db_tbl_, const String & disk_name_, const String & dict_path_, ContextPtr context);
    ~BitEngineDictionaryManager() override;
    BitEngineDictionaryPtr getBitEngineDictPtr(const String & column) override;

    void reload(const String & column_name) override;
    void flushDict() override;

    void setVersion(const size_t version_);
    void updateVersionTo(const size_t version_);
    void updateVersion() override;
    void loadVersion();
    void flushVersion();
    size_t getVersion() const { return version; }
    void updateSnapshots();


    IncrementOffset getIncrementOffset();
//    IncrementOffset getEmptyIncrementOffset();
    IncrementData getIncrementData(const IncrementOffset & increment_offset);
    void insertIncrementData(const IncrementData & increment_data);
    void readDataFromReadBuffer(ReadBuffer & in);
    void writeDataToWriteBuffer(WriteBuffer & out);

    struct Status
    {
        UInt64 version = 0;
        Strings encoded_columns;
        std::vector<UInt64> encoded_columns_size;
        bool is_valid = 0;
        UInt64 shard_id = 0;
        UInt64 shard_base_offset{0};
    };

    Status getStatus();
    std::map<String, UInt64> getAllDictColumnSize() override;
    void resetDict() { resetDictImpl(); } // used in repair mode where dicts in all replicas are corrupted.

//    MergeTreeData::AlterDataPartTransactionPtr
//    recodeBitEnginePartInTransaction(const MergeTreeData::DataPartPtr & part,
//                                     const NamesAndTypesList & columns,
//                                     const MergeTreeData & merge_tree_data,
//                                     bool can_skip = false,
//                                     bool without_lock = false) override;

    bool checkEncodedPart(const MergeTreeData::DataPartPtr & part,
                          const MergeTreeData & merge_tree_data,
                          std::unordered_map<String, MergeTreeData::DataPartPtr> & res_abnormal_parts,
                          bool without_lock = false) override;
    MergeTreeData::DataPartsVector checkEncodedParts(const MergeTreeData::DataPartsVector & parts,
                                                     const MergeTreeData & merge_tree_data,
                                                     ContextPtr query_context,
                                                     bool without_lock = false) override;

private:
        void init();
        void resetDictImpl();

        String version_path;
        size_t version = 0;
        std::map<String, BitEngineDictionarySnapshot> dict_snapshots;  // <column_name, bitengine_dictioanry>

        // Lock for snapshots
        mutable RWLock snapshot_lock = RWLockImpl::create();
        using SnapshotLock = RWLockImpl::LockHolder;
        SnapshotLock readLockForSnapshot(const std::string & who = RWLockImpl::NO_QUERY)
        {
            auto res = snapshot_lock->getLock(RWLockImpl::Read, who);
            return res;
        }

        SnapshotLock writeLockForSnapshot(const std::string & who = RWLockImpl::NO_QUERY)
        {
            auto res = snapshot_lock->getLock(RWLockImpl::Write, who);
            return res;
        }

        Poco::Logger * log;
};

using BitEngineDictionaryManagerPtr = std::shared_ptr<IBitEngineDictionaryManager>;



/////////////   StartOf BitEngineDictionaryHaManager


class BitEngineDictionaryHaManager
{
public:
    BitEngineDictionaryHaManager(StorageHaMergeTree & storage, BitEngineDictionaryManager * bitengine_manager, const String & zookeeper_path, const String & replica_name);
    ~BitEngineDictionaryHaManager();
    zkutil::ZooKeeperPtr getZooKeeper();
    String getReplicaPath() { return replica_path; }
    String getReplicaPath(const String & replica) { return zookeeper_path + "/replicas/" + replica; }

    std::tuple<size_t, String> getMaxVersionAndReplica();
    size_t getMinVersion();
    size_t getVersionOnZooKeeper();
    void updateVersionOnZookeeper();
    void updateVersion();
    void tryUpdateDictFromReplica(const String & replica);
    void tryUpdateDictFromReplicaPath(const String & src_replica_path);
    void tryUpdateDict();
    void resetDict() { resetDictImpl(); } // used in repair mode where dicts in all replicas are corrupted.

    void tryUpdateVersionAndDict();
    String getDatabaseAndTable();

    void readData(ReadBuffer & in);
    void writeData(WriteBuffer & out);
    void readIncrementData(ReadBuffer & in);
    void writeIncrementData(WriteBuffer & out, const IncrementOffset & increment_offset);
    IncrementOffset readIncrementOffset(ReadBuffer & in);
    void writeIncrementOffset(WriteBuffer & out);

    bool isValid() {
        if (bitengine_manager)
            return is_valid && bitengine_manager->isValid();
        return false;
    }
    void setInvalid();
    void setValid();
    void flush();

    struct BitEngineLock
    {
        BitEngineLock(BitEngineDictionaryHaManager & ha_manager, const String & lock_path);
        ~BitEngineLock();
        BitEngineDictionaryHaManager & ha_manager;
        String lock_path;
    };

    using BitEngineLockPtr = std::shared_ptr<BitEngineLock>;

    BitEngineLockPtr tryGetLock();

    void stop();
    bool isStopped() { return stopped; }

    void checkBitEnginePart(const MergeTreeData::DataPartPtr & part);
    bool recodeBitEnginePart(const MergeTreeData::MutableDataPartPtr & part, bool can_skip = false, bool without_lock = false);
    bool recodeBitEngineParts(const MergeTreeData::MutableDataPartsVector & parts, bool can_skip = false, bool without_lock = false);
    bool recodeBitEnginePartsParallel(MergeTreeData::MutableDataPartsVector & parts, ContextPtr query_context, bool can_skip = false);
    MergeTreeData::DataPartsVector checkEncodedParts(const MergeTreeData::DataPartsVector & parts, ContextPtr query_context, bool without_lock = false);

private:
    friend class BitEngineDataExchanger;
    friend class BitEngineDataService;

    void prepareZookeeper();
    Strings getReplicas();
    HaMergeTreeAddress getReplicaAddress(const String & replica_name_);
    Strings getActiveReplicas();
    bool isActiveReplica(const String & replica);
    bool isActiveReplica(const String & replica, zkutil::ZooKeeperPtr & zookeeper);
    size_t getVersionOfReplica(const String & replica, zkutil::ZooKeeperPtr & zookeeper);
    void setVersionOnZookeeper();
    void resetDictImpl();

    StorageHaMergeTree & storage;
    BitEngineDictionaryManager * bitengine_manager;

    String zookeeper_path;
    String replica_name;
    String replica_path;
    String version_path;
    String lock_path;
    bool is_valid = true;

    Poco::Logger * log;

    size_t version = 0;
    bool stopped = false;
    zkutil::ZooKeeperPtr current_zookeeper;
    std::mutex ha_mutex;
    std::mutex lock_mutex;

    BitEngineDataExchangerPtr bitengine_dict_exchanger;
    InterserverIOEndpointPtr bitengine_dict_endpoint;

    zkutil::EventPtr event = std::make_shared<Poco::Event>();
};
}
