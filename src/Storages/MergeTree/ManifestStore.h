#pragma once

#include <map>
#include <mutex>
#include <rocksdb/db.h>
#include <Core/Types.h>
#include <IO/Operators.h>
#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MANIFEST_OPERATION_ERROR;
}

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
        MAX = CHECKPOINT_MANIFEST /// Max element of enum Type, remember update this after add new element
    };

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
            default: throw Exception("Unknown manifest log entry type: " + toString<int>(type), ErrorCodes::LOGICAL_ERROR);
        }
        // clang-format on
    }

    static constexpr UInt8 format_version = 1;

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

    /// for MODIFY_SCHEMA
    String metadata_str;    /// table schema metadata
    String columns_str;     /// table schema columns

    static void checkConsecutiveLogs(const std::vector<ManifestLogEntry> & entries);

    String toText() const;
    void sanityCheck() const;
    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
};

struct ManifestStatus
{
    UInt8 is_leader;
    UInt64 latest_version;
    UInt64 commit_version;
    UInt64 checkpoint_version;
    /// number of active part/delete file sending thread on this server,
    /// used as a rough metric to measure server load
    UInt64 num_running_sends;
};

class ManifestStore
{
public:
    using Version = UInt64;
    using LogEntries = std::vector<ManifestLogEntry>;
    static constexpr auto MAX_VERSION = 9999999999;
    static constexpr auto INVALID_VERSION = MAX_VERSION + 1;

    ManifestStore(String dir_, const String & logger_name);
    ~ManifestStore();

    /// open manifest and load metadata, create if not exist
    void open(const String& metadata, const String& columns);

    /// close the manifest DB
    void close();

    using WriteLock = std::unique_lock<std::mutex>;
    WriteLock writeLock() const { return WriteLock(mutex_write); }

    /// Append the given logs. The first log's prev_version must match manifest's latest version.
    /// If commit is true, also commit new logs, but it requires that all previous logs were committed.
    void append(const WriteLock &, const LogEntries & entries, bool commit);
    void append(const WriteLock & lock, const ManifestLogEntry & entry, bool commit) { append(lock, LogEntries{ entry }, commit); }

    struct Snapshot
    {
        /// part name -> delete version
        std::map<String, Version> parts;

        /// The latest table schema in this snapshot
        String metadata_str;
        String columns_str;
    };

    /// Note that `version' must be >= last checkpoint version, otherwise exception is thrown
    Snapshot getSnapshot(Version version) const;

    /// Return log entries whose version >= from in ascending order.
    /// If limit > 0, at most `limit' entries is returned.
    /// error if from < checkpointVersion
    LogEntries getLogEntries(Version from, size_t limit = 0) const;

    /// Return whether the manifest contains log for the given version.
    /// If entry is not nullptr, also set *entry to the log content if found.
    bool containsLog(Version version, ManifestLogEntry * entry = nullptr) const;

    Version latestVersion() const { return latest_version.load(std::memory_order_acquire); }
    Version commitVersion() const { return commit_version.load(std::memory_order_acquire); }
    Version checkpointVersion() const { return checkpoint_version.load(std::memory_order_acquire); }

    /// Return create time of last commit log entry, 0 if no logs are committed.
    time_t timeOfLastCommitLog() const { return time_of_last_commit_log.load(std::memory_order_relaxed); }

    /// no-op if version <= checkpointVersion,
    /// error if version > commitVersion
    void checkpointTo(Version version);
    /// Commit the log for the given version.
    /// Precondition: all logs < version must be committed
    /// Error if the log is not found or its prev version doesn't match commitVersion
    void commitLog(const WriteLock &, Version version);
    /// remove all logs >= from
    void discardFrom(const WriteLock &, Version from);

    /// Reset manifest to checkpoint at `version' containing `snapshot'.
    /// Note that it will discard the current content of manifest, use with caution!
    /// Post-condition: checkpointVersion == commitVersion == latestVersion == `version'
    void resetToCheckpoint(Version version, const Snapshot & snapshot);

    String stateSummary() const;

    /// Remember in-memory that the specified parts was removed at the given version.
    /// Note that commit an log entry with removed part will implicitly call this method.
    void putPartRemovedVersion(const Strings & parts, Version version);
    /// Returns the version at which the given part is removed
    /// or INVALID_VERSION if the removed version hasn't been cached yet.
    Version getPartRemovedVersion(const String & part_name) const;
    /// Removes the given parts from in-memory removed version cache
    void deletePartRemovedVersion(const Strings & parts);
    /// Clear in-memory removed version cache
    void clearAllPartRemovedVersion();

    const String & getLatestTableMetadata() const { return metadata_str; }
    const String & getLatestTableColumns() const { return columns_str; }

private:
    void checkOpened() const
    {
        if (db == nullptr)
            throw Exception("Illegal state: manifest DB for " + dir + " is not opened", ErrorCodes::LOGICAL_ERROR);
    }
    static void checkVersionIsValid(Version version)
    {
        if (version > MAX_VERSION)
            throw Exception("Invalid manifest log version " + toString(version) + ", max is " + toString(MAX_VERSION), ErrorCodes::LOGICAL_ERROR);
    }

    void checkVersionInvariant()
    {
        if (!(checkpoint_version <= commit_version && commit_version <= latest_version))
            throw Exception("Against version invariant: " + stateSummary(), ErrorCodes::LOGICAL_ERROR);
    }

    static constexpr auto log_key_prefix = "log:";
    static constexpr auto latest_version_key = "log:latest_version";
    static constexpr auto commit_version_key = "log:commit_version";
    static constexpr auto checkpoint_version_key = "log:checkpoint_version";
    /// the following two keys are used to store the latest table schema
    static constexpr auto latest_metadata_key = "log:latest_metadata";
    static constexpr auto latest_columns_key = "log:latest_columns";
    static String generateLogKey(Version version);

    inline String formatFilePath(String path, Version version) const;
    inline String checkpointFilePath(Version version) const;
    inline String metadataFilePath(Version version) const;
    inline String columnsFilePath(Version version) const;

    using CheckpointLock = std::unique_lock<std::mutex>;
    CheckpointLock checkpointLock() const { return CheckpointLock(mutex_checkpoint); }

    Snapshot getSnapshotImpl(
        const CheckpointLock & checkpoint_lock, Version version,
        String * delta_start_key = nullptr, String * delta_end_key = nullptr) const;

    void writeCheckpointFile(Version version, const Snapshot & snapshot) const;
    Snapshot readCheckpointFile(Version version) const;

    bool lookForLatestTableSchema();

    String dir;
    Poco::Logger * logger;
    rocksdb::DB * db;
    std::atomic<time_t> time_of_last_commit_log;
    /// invariant: checkpoint_version <= commit_version <= latest_version
    std::atomic<Version> latest_version;
    std::atomic<Version> commit_version;
    std::atomic<Version> checkpoint_version;
    /// lock should be hold in the order below
    /// used to prevent concurrent checkpoint
    mutable std::mutex mutex_checkpoint;
    /// used to prevent concurrent append/commit
    mutable std::mutex mutex_write;

    mutable std::mutex mutex_removed_cache;
    /// part name -> version at which the part is removed, protected by `mutex_removed_cache'
    /// used by the cleanup thread to determine whether an outdated part can be deleted
    std::map<String, Version> removed_parts_cache;

    /// Cache the value of `latest_metadata_key' and `latest_columns_key'.
    /// Should be updated whenever the values of `latest_metadata_key' and `latest_columns_key' are changed.
    String metadata_str;
    String columns_str;
};
using ManifestStorePtr = std::unique_ptr<ManifestStore>;

}
