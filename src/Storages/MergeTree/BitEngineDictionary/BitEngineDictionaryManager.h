#pragma once

#include <Storages/MergeTree/BitEngineDictionary/IBitEngineDictionaryManager.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionarySnapshot.h>

#include <Interpreters/InterserverIOHandler.h>
#include <Common/RWLock.h>
#include <common/logger_useful.h>

// TODO (liuhaoqiang) remove it after all function finished
#pragma  GCC diagnostic ignored  "-Wunused"
#pragma GCC diagnostic ignored "-Wunused-parameter"

namespace DB
{
class MergeTreeData;
class BitEngineDictionarySnapshot;
class BitEngineDataService;
class BitEngineDictionary;
class IncrementOffset;
class IncrementData;

using BitEngineDictionaryPtr = std::shared_ptr<BitEngineDictionary>;
using BitEngineDictionarySnapshotPtr = std::shared_ptr<BitEngineDictionarySnapshot>;

class BitEngineDictionaryManager final : public BitEngineDictionaryManagerBase<BitEngineDictionaryPtr>
{
public:
    BitEngineDictionaryManager(const String & db_tbl_, const String & disk_name_, const String & dict_path_, ContextPtr context);
    ~BitEngineDictionaryManager() override;
    BitEngineDictionaryPtr getBitEngineDictPtr(const String & column) override;

    BitEngineDictionarySnapshot getDictSnapshot(const String & column_name);
    BitEngineDictionarySnapshotPtr getDictSnapshotPtr(const String & column_name);
    BitEngineDictionarySnapshotPtr tryGetUpdatedSnapshot(const String & column_name);
    void updateSnapshots();

    void reload(const String & column_name) override;
    void flushDict() override;

    void setVersion(const size_t version_);
    void updateVersionTo(const size_t version_);
    void updateVersion() override;
    void loadVersion();
    void flushVersion();
    size_t getVersion() const { return version; }

    ColumnPtr decodeColumn(const IColumn & column, const String & dict_name);
    ColumnPtr decodeNonBitEngineColumn(const IColumn & column, String & dict_name);

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

    struct RetrieveKey
    {
        template <typename T>
        typename T::first_type operator()(T keyValuePair) const
        {
            return keyValuePair.first;
        }
    };

    Strings getDictKeysVector();
    String allDictNamesToString();

    MergeTreeData::MutableDataPartPtr
    encodePartToTemporaryPart(
        const FutureMergedMutatedPart & future_part,
        const NamesAndTypesList & encode_columns,
        const MergeTreeData & merge_tree_data,
        const ReservationPtr & space_reservation,
        bool can_skip = false,
        bool part_in_detach = true,
        bool without_lock = false) override;
    void finalizeEncodedPart(
        const MergeTreeDataPartPtr & source_part,
        MergeTreeData::MutableDataPartPtr new_data_part,
        bool need_remove_expired_values,
        const CompressionCodecPtr & codec);
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
        std::unordered_map<String, BitEngineDictionarySnapshotPtr> dict_snapshots;  // <column_name, bitengine_dictioanry>

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

}
