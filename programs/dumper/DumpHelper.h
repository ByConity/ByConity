// #include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Common/Logger.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <rocksdb/db.h>

namespace DB
{
class Context;

struct ManifestLogEntry
{
    enum Type
    {
        EMPTY, /// illegal type, used as the default value to catch bug
        SYNC_PARTS,
        DETACH_PARTS,
        RESET_MANIFEST,
        CHECKPOINT_MANIFEST,
        MODIFY_SCHEMA,
        SYNC_ROCKSDB_FILES,
        MAX = SYNC_ROCKSDB_FILES /// Max element of enum Type, remember update this after add new element
    };

    ManifestLogEntry() = default;

    static String typeToString(Type type)
    {
        // clang-format off
        switch (type)
        {
            case SYNC_PARTS: return "SYNC_PARTS";
            case DETACH_PARTS: return "DETACH_PARTS";
            case RESET_MANIFEST: return "RESET_MANIFEST";
            case CHECKPOINT_MANIFEST: return "CHECKPOINT_MANIFEST";
            case MODIFY_SCHEMA: return "MODIFY_SCHEMA";
            case SYNC_ROCKSDB_FILES: return "SYNC_ROCKSDB_FILES";
            default: throw Exception("Unknown manifest log entry type: " + toString<int>(type), ErrorCodes::LOGICAL_ERROR);
        }
        // clang-format on
    }

    const UInt8 format_version = 1ULL;

    /// required fields
    Type type = EMPTY;
    UInt64 version = 0;
    UInt64 prev_version = 0;
    time_t create_time = 0; /// zero means auto generate when serialize
    String source_replica;  /// records the leader replica which generated the log

    /// for SYNC_PARTS
    Strings added_parts;    /// new parts
    Strings updated_parts;  /// parts whose delete file is updated
    Strings removed_parts;  /// parts removed either directly or being covered by new parts

    /// for RESET_MANIFEST and CHECKPOINT_MANIFEST
    UInt64 target_version = 0;

};

/// part name -> delete version
using Snapshot = std::map<String, UInt64>;

using SerializationsMap = std::unordered_map<String, SerializationPtr>;

struct ManifestStore {
    static constexpr auto MAX_VERSION = 9999999999;
    static constexpr auto INVALID_VERSION = MAX_VERSION + 1;
    static constexpr auto log_key_prefix = "log:";
    static constexpr auto commit_version_key = "log:commit_version";
    static constexpr auto checkpoint_version_key = "log:checkpoint_version";
    static String generateLogKey(UInt64 version)
    {
        checkVersionIsValid(version);
        std::ostringstream ss;
        ss << log_key_prefix << std::setfill('0') << std::setw(10) << version;
        return ss.str();
    }
    static void checkVersionIsValid(UInt64 version)
    {
        if (version > MAX_VERSION)
            throw Exception("Invalid manifest log version " + toString(version) + ", max is " + toString(MAX_VERSION), ErrorCodes::LOGICAL_ERROR);
    }
    static void readLabeledNames(ReadBuffer & in, const char * label, Strings & names);
    Snapshot readCheckpointFile(UInt64 version, const String & path);
    void readManifest(ManifestLogEntry & log_entry, ReadBuffer & in) const;
};

class UniqueTableDumpHelper
{
public:

    void parseZKReplicaPathFromEngineArgs(const ASTStorage * storage, const String & database, const String & table, const Context & context);

    void loadAndDumpDeleteBitmap(StorageCloudMergeTree & cloud, MergeTreeCloudData::DataPartPtr part, const UInt64 & version, const std::shared_ptr<IDisk> & local_disk = nullptr);

    void generateUniqueIndexFileIfNeed(MergeTreeMetaBase::MutableDataPartPtr & part, StorageCloudMergeTree & cloud, const std::shared_ptr<IDisk> & local_disk);

    Snapshot getUniqueTableSnapshot(const String & database, const String & table, const String & dir, const Context & context);

    void removeDumpVersionFromZk(const Context & context);

    void setLog(LoggerPtr log_) { log = log_; }

private:
    void writeTempUniqueKeyIndex(Block & block, size_t first_rid, rocksdb::DB & temp_index, StorageCloudMergeTree & cloud);
    void saveDumpVersionToZk(const UInt64 & dump_version, const Context & context);

    String replica_path;
    String unique_version_column;
    String dump_lsn_path;
    ManifestStore manifest_store;
    LoggerPtr log;
};

}
