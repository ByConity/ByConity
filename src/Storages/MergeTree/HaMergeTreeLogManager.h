#pragma once

#include <mutex>
#include <set>
#include <shared_mutex>
#include <boost/noncopyable.hpp>

#include <Storages/MergeTree/HaMergeTreeLogEntry.h>
#include <Storages/MergeTree/LSNStatus.h>
#include <common/logger_useful.h>

namespace DB
{
class ReadBufferFromFile;
class WriteBufferFromFile;
class StorageHaMergeTree;

/**
 *  Log manager holds the uncommitted log entries of which contents are exactly
 *  same with those log entries on disk. In order to debug more easily, the
 *  disk would keep more log entries than log manager.
 */

class HaMergeTreeLogManager : public boost::noncopyable
{
    friend class HaMergeTreeQueue;

public:
    using LogEntry = HaMergeTreeLogEntry;
    using LogEntryPtr = LogEntry::Ptr;
    using LogEntryList = std::set<LogEntryPtr, LogEntry::LSNLessCompare>;

    enum Status
    {
        UNINITIALIZED,
        INTEGRATED,
        CORRUPTED
    };

private:
    static constexpr UInt8 VERSION = 1;
    static constexpr UInt8 MULTI_STORAGE_VERSION = 1;

    struct LogIndexFileHeader
    {
        UInt8 version;
        UInt32 count;
        UInt64 committed_lsn;
        UInt64 max_lsn;

        UInt64 checksum;
        UInt64 data_checksum;
        // UInt64 padding_6;
        // UInt64 padding_7;
    } __attribute__((__aligned__(64)));
    static_assert(sizeof(LogIndexFileHeader) == 64);
    static_assert(std::is_pod<LogIndexFileHeader>::value);

    struct LogIndex
    {
        UInt64 lsn;
        UInt32 offset; /// log file must be less than 4GiB
        UInt32 len;

        bool operator==(const LogIndex & rhs) const { return lsn == rhs.lsn && offset == rhs.offset && len == rhs.len; }
        bool operator!=(const LogIndex & rhs) const { return !operator==(rhs); }

        struct LSNLessCompare
        {
            using is_transparent = void;
            bool operator()(const LogIndex & lhs, const LogIndex & rhs) const { return lhs.lsn < rhs.lsn; }
            bool operator()(const LogIndex & lhs, UInt64 rhs) const { return lhs.lsn < rhs; }
            bool operator()(UInt64 lhs, const LogIndex & rhs) const { return lhs < rhs.lsn; }
        };
    };
    static_assert(std::is_pod<LogIndex>::value);

    using IndexList = std::set<LogIndex, LogIndex::LSNLessCompare>;
    using IndexVec = std::vector<LogIndex>;

    using ReadBufferFromFilePtr = std::unique_ptr<ReadBufferFromFile>;
    using WriteBufferFromFilePtr = std::unique_ptr<WriteBufferFromFile>;

    struct LogData
    {
        LogIndexFileHeader header{};
        IndexList indexes{};
        LogEntryList entries{};
    };

    enum LogFileType
    {
        MAIN,
        MIRROR
    };

    struct LogFiles
    {
        String entry_filename;
        String index_filename;

        ReadBufferFromFilePtr entry_rbuf;
        ReadBufferFromFilePtr index_rbuf;
        WriteBufferFromFilePtr entry_wbuf;
        WriteBufferFromFilePtr index_wbuf;

        bool ok{false};
    };

private:
    std::mutex op_mutex; /// lock entire class

    std::shared_mutex mem_mutex; /// lock entries & indexes & header

    StorageHaMergeTree & storage;
    LogIndexFileHeader index_header{};
    IndexList indexes{};
    LogEntryList entries{};

    UInt64 cached_updated_lsn;
    LogEntryList::const_iterator cached_updated_iter;

    String log_path{};
    LogFiles main_files;
    LogFiles mirror_files;

    std::atomic<Status> status{UNINITIALIZED};

    Poco::Logger * log{nullptr};

    void checkStatus(Status expected, const char * func);

    void createOrOpen(bool create);

    void recoverFrom(LogFileType type);
    void saveLogfile();

    bool checkLogDataWithMemory(LogData & data);

    void tryRecover(bool check_log_file_with_mem);

    static LogData loadDataFromFileImpl(LogFiles & files);
    LogData loadDataFromFile(LogFiles & files);

    static LogIndexFileHeader calcNewIndexHeader(const LogIndexFileHeader & header, const std::vector<LogIndex> & indexes);

    static void checkNewIndexes(const IndexVec & lhs, const IndexVec & rhs);

    static IndexVec writeLogEntriesToFile(
        LogFiles & files, const std::vector<LogEntryPtr> & entries, const LogIndexFileHeader & header, std::lock_guard<std::mutex> & lock);

    static IndexVec rewriteLogEntriesToFile(
        LogFiles & files, const std::vector<LogEntryPtr> & entries, const LogIndexFileHeader & header, std::lock_guard<std::mutex> & lock);

    void execWriteAction(
        const char * func,
        const std::function<void(LogFiles & files)> & main_action,
        const std::function<void(LogFiles & files)> & mirror_action,
        const std::function<void()> & check_result = {});

    std::vector<HaMergeTreeLogManager::LogEntryPtr> writeLogEntries(std::vector<LogEntryPtr> entries);
    bool writeLogEntry(const LogEntryPtr & entry);
    bool writeLogEntry(const LogEntry & entry);

    UInt64 calcUpdatedLSNUnsafe();

public:
    explicit HaMergeTreeLogManager(const String & log_path_, const String & logger_name_, bool create_, StorageHaMergeTree & storage_);
    ~HaMergeTreeLogManager();

    void setLogPath(const String & log_path_);
    void setLoggerName(const String & logger_name_);

    /// Create (and initialize) log files in filesystem
    void create();

    /// Check whether log files exist in filesystem
    bool exists();

    /// Load log entry data
    void load();

    /// Remove from filesystem
    void drop();

    Status getStatus() const { return status; }

    static String statusToString(Status s);

    auto lockMe() { return std::shared_lock(mem_mutex); }
    LSNStatus getLSNStatusUnsafe(bool require_updated_lsn = false);
    LSNStatus getLSNStatus(bool require_updated_lsn = false);
    const LogEntryList & getEntryListUnsafe() const;

    void applyToEntriesFromUpdatedLSNUnsafe(const std::function<void(const LogEntryPtr &)> & f);

    void markLogEntryExecuted(UInt64 lsn);
    void markLogEntriesExecuted(const std::vector<UInt64> & lsns);
    void markLogEntriesExecuted(const LogEntry::Vec & entries);

    UInt64 calcMaxExecutedLSN();

    void commitTo(UInt64 lsn_to_commit);
    void discardTo(UInt64 lsn_to_discard);
    void resetTo(UInt64 lsn_to_reset);

    /// bool checkTransactionLog(const TransactionID & xid, const HaMergeTreeLogEntry::Type & type, const TransactionStatus & tran_status);

    /// bool completeTransaction(const TransactionID & xid, const HaMergeTreeLogEntry::Type & type, UInt64 terminate_item_index);

    /// bool verifyPartCanMerged(const Strings & parts_need_merged);

    /// void updateLogTransaction(UInt64 lsn, TransactionStatus tran_status);

    const String & getLogPath() const { return log_path; }
};

} // end of namespace DB
