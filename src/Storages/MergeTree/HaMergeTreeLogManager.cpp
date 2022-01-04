#include <Storages/MergeTree/HaMergeTreeLogManager.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Storages/StorageHaMergeTree.h>

// #include <Interpreters/MergeTreeTransactionItem.h>
// #include <Interpreters/TransactionManager.h>

#include <Poco/File.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;

extern const int UNSUPPORTED_LOG_VERSION;
extern const int CORRUPTED_LOG_FILE;
extern const int INCONSISTENT_LOG_DATA;
extern const int CANNOT_WRITE_TO_HA_LOG;
}

namespace {
/// Small buffer is enough
constexpr size_t LOG_ENTRY_FILE_BUFFER_SIZE = 4 * 1024;
constexpr size_t LOG_INDEX_FILE_BUFFER_SIZE = 4 * 1024;

void overwriteFile(ReadBufferFromFile & source, WriteBufferFromFile & target)
{
    target.seek(0, SEEK_SET);
    source.seek(0, SEEK_SET);
    copyData(source, target);
    target.next();
    target.truncate(target.getPosition());
    target.sync();

    target.seek(0, SEEK_END);
}

}

/**
 * Be careful with WriteBufferFromFile! some operations may not act as thought,
 * Make sure that call next()/sync() before writing operations like seek(),
 * otherwise the file data may be messy.  The getPosition() may not return
 * the actual write position because of buffer. And truncate() maybe does no effect.
 *
 * In a word, keep mind that there is a buffer which may not be flushed;
 * So make sure placing next()/sync() in the right position.
 */

HaMergeTreeLogManager::HaMergeTreeLogManager(
    const String & log_path_, const String & logger_name_, bool create_, StorageHaMergeTree & _storage)
    : storage(_storage), cached_updated_lsn(0), cached_updated_iter(entries.end())
{
    setLogPath(log_path_);
    setLoggerName(logger_name_);

    if (create_)
        create();
}

HaMergeTreeLogManager::~HaMergeTreeLogManager()
{
}

void HaMergeTreeLogManager::setLogPath(const String & log_path_)
{
    std::lock_guard guard(op_mutex);
    log_path = log_path_;
    main_files.entry_filename = log_path + "/log.bin";
    main_files.index_filename = log_path + "/log.idx";
    mirror_files.entry_filename = log_path + "/mirror_log.bin";
    mirror_files.index_filename = log_path + "/mirror_log.idx";
}

void HaMergeTreeLogManager::setLoggerName(const String & logger_name_)
{
    log = &Poco::Logger::get(logger_name_);
}

void HaMergeTreeLogManager::createOrOpen(bool create)
{
    int write_flags = O_WRONLY;
    if (create)
        write_flags |= O_CREAT | O_TRUNC;

    auto create_or_open_impl = [&] (LogFiles & files)
    {
        /// Always reopen files
        files.entry_wbuf = std::make_unique<WriteBufferFromFile>(files.entry_filename, LOG_ENTRY_FILE_BUFFER_SIZE, write_flags, 0600);
        files.index_wbuf = std::make_unique<WriteBufferFromFile>(files.index_filename, LOG_INDEX_FILE_BUFFER_SIZE, write_flags, 0600);
        files.entry_wbuf->seek(0, SEEK_END);
        files.index_wbuf->seek(0, SEEK_END);

        files.entry_rbuf = std::make_unique<ReadBufferFromFile>(files.entry_filename, LOG_ENTRY_FILE_BUFFER_SIZE);
        files.index_rbuf = std::make_unique<ReadBufferFromFile>(files.index_filename, LOG_INDEX_FILE_BUFFER_SIZE);

        files.ok = true;
    };

    create_or_open_impl(main_files);
    create_or_open_impl(mirror_files);

    auto init_index_head_if_needed = [&] (LogFiles & files)
    {
        if (Poco::File(files.index_filename).getSize() >= sizeof(LogIndexFileHeader))
            return;

        /// index is uninitialized or the header is not flushed into disk, take it again
        index_header.version = VERSION;
        index_header.committed_lsn = 0;
        index_header.max_lsn = 0;
        index_header.count = 0;

        files.index_wbuf->seek(0, SEEK_SET);
        writePODBinary(index_header, *files.index_wbuf);
        files.index_wbuf->next();
    };

    init_index_head_if_needed(main_files);
    init_index_head_if_needed(mirror_files);
}

void HaMergeTreeLogManager::checkStatus(Status expected, const char * func)
{
    if (status != expected)
    {
        throw Exception(String(func) + ": expect log status " + statusToString(expected) + " but current " + statusToString(status),
                ErrorCodes::LOGICAL_ERROR);
    }
}

void HaMergeTreeLogManager::create()
{
    std::lock_guard guard(op_mutex);

    checkStatus(UNINITIALIZED, __func__);

    /// Create log files
    Poco::File(log_path).createDirectory();
    createOrOpen(true);

    status = INTEGRATED;
}

bool HaMergeTreeLogManager::exists()
{
    return (Poco::File(main_files.entry_filename).exists()
            && Poco::File(main_files.index_filename).exists()
            && Poco::File(mirror_files.entry_filename).exists()
            && Poco::File(mirror_files.index_filename).exists());
}

HaMergeTreeLogManager::LogData HaMergeTreeLogManager::loadDataFromFileImpl(LogFiles & files)
{
    auto & index_rbuf = files.index_rbuf;
    auto & entry_rbuf = files.entry_rbuf;

    LogData data;

    /// Index header
    index_rbuf->seek(0, SEEK_SET);
    readPODBinary(data.header, *index_rbuf);
    if (data.header.version != VERSION || data.header.version != MULTI_STORAGE_VERSION)
    {
        throw Exception("Unsupported log file version: " + toString(data.header.version) + " " + index_rbuf->getFileName(),
                ErrorCodes::UNSUPPORTED_LOG_VERSION);
    }

    /// indexes and entries
    entry_rbuf->seek(0, SEEK_SET);

    for (size_t i = 0; i < data.header.count; ++i)
    {
        LogIndex index;
        readPODBinary(index, *index_rbuf);

        if (entry_rbuf->getPosition() != index.offset)
        {
            throw Exception("The index offset " + toString(index.offset)
                    + " != FP of log file " + toString(entry_rbuf->getPosition())
                    + " " + entry_rbuf->getFileName(),
                    ErrorCodes::CORRUPTED_LOG_FILE);
        }

        LogEntry entry;
        entry.readText(*entry_rbuf);

        /// Only load the uncommitted log entry
        if (index.lsn > data.header.committed_lsn)
        {
            data.indexes.insert(index);
            data.entries.insert(std::make_shared<LogEntry>(entry));
        }
    }

    auto check_tail = [&] (ReadBufferFromFilePtr & rbuf, WriteBufferFromFilePtr & wbuf, const char * type)
    {
        if (rbuf->getPosition() != wbuf->getPosition())
        {
            throw Exception("Unexpected data in the tail of " + toString(type) + " file: "
                    + toString(rbuf->getPosition()) + " < " + toString(wbuf->getPosition())
                    + " " + entry_rbuf->getFileName(),
                    ErrorCodes::CORRUPTED_LOG_FILE);
        }
    };

    check_tail(index_rbuf, files.index_wbuf, "index");
    check_tail(entry_rbuf, files.entry_wbuf, "entry");

    return data;
}

HaMergeTreeLogManager::LogData HaMergeTreeLogManager::loadDataFromFile(LogFiles & files)
{
    try
    {
        return loadDataFromFileImpl(files);
    }
    catch (Poco::Exception &)
    {
        files.ok = false;
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return {};
    }
}

void HaMergeTreeLogManager::load()
{
    std::lock_guard guard(op_mutex);

    /// checkStatus(UNINITIALIZED, __func__);

    /// Load log files
    createOrOpen(false);

    auto main_data = loadDataFromFile(main_files);
    auto mirror_data = loadDataFromFile(mirror_files);

    if (main_files.ok && mirror_files.ok)
    {
        /// Both are integrate, some data may be only written to main log file.
        /// Mark the mirror log file as failed
        if (0 != memcmp(&main_data.header, &mirror_data.header, sizeof(LogIndexFileHeader))
            || main_data.indexes != mirror_data.indexes)
        {
            mirror_files.ok = false;
            LOG_WARNING(log, "Loading log: inconsistent main and mirror log index: " + log_path);
        }

        if (!std::equal(main_data.entries.begin(), main_data.entries.end(), mirror_data.entries.begin(), mirror_data.entries.end(),
            [](const LogEntryPtr & lhs, const LogEntryPtr & rhs)
            {
                return lhs->toString() == rhs->toString();
            }))
        {
            mirror_files.ok = false;
            LOG_WARNING(log, "Loading log: inconsistent main and mirror log entry: " + log_path);
        }
    }

    {
        /// MUST set data in memory correctly before recover log file
        /// due to the recovery would compare the data in memory and disk.

        /// Also require mem lock.
        std::lock_guard mem_lock(mem_mutex);

        if (main_files.ok)
        {
            index_header = main_data.header;
            indexes = std::move(main_data.indexes);
            entries = std::move(main_data.entries);
        }
        else
        {
            index_header = mirror_data.header;
            indexes = std::move(mirror_data.indexes);
            entries = std::move(mirror_data.entries);
        }

        /// initialize variables related updated_lsn
        cached_updated_lsn = index_header.committed_lsn;
        cached_updated_iter = entries.end();
        calcUpdatedLSNUnsafe();
    }

    /// Will throw exception in tryRecover() if both are corrupted
    if (!main_files.ok || !mirror_files.ok)
    {
        tryRecover(true);
    }

    status = INTEGRATED;

    LOG_DEBUG(log, "Loaded log entry data.");
}

void HaMergeTreeLogManager::drop()
{
    std::lock_guard guard(op_mutex);
    Poco::File(log_path).remove(true);

    status = UNINITIALIZED;
}

String HaMergeTreeLogManager::statusToString(Status s)
{
    switch (s)
    {
        case UNINITIALIZED:
            return "Uninitialized";
        case INTEGRATED:
            return "Integrated";
        case CORRUPTED:
            return "Corrupted";
    }
}

LSNStatus HaMergeTreeLogManager::getLSNStatusUnsafe(bool require_updated_lsn)
{
    LSNStatus lsn_status;
    lsn_status.max_lsn = index_header.max_lsn;
    lsn_status.committed_lsn = index_header.committed_lsn;
    lsn_status.count = index_header.count;
    if (require_updated_lsn)
        lsn_status.updated_lsn = calcUpdatedLSNUnsafe();
    return lsn_status;
}

LSNStatus HaMergeTreeLogManager::getLSNStatus(bool require_updated_lsn)
{
    std::lock_guard mem_lock(mem_mutex);
    return getLSNStatusUnsafe(require_updated_lsn);
}

const HaMergeTreeLogManager::LogEntryList & HaMergeTreeLogManager::getEntryListUnsafe() const
{
    return entries;
}

void HaMergeTreeLogManager::applyToEntriesFromUpdatedLSNUnsafe(const std::function<void(const LogEntryPtr &)> & f)
{
    calcUpdatedLSNUnsafe();

    LogEntryList::const_iterator begin = (cached_updated_iter == entries.end()) ? entries.begin() : cached_updated_iter;
    for (auto it = begin; it != entries.end(); ++it)
        f(*it);
}

void HaMergeTreeLogManager::recoverFrom(LogFileType type)
{
    saveLogfile();
    try
    {
        if (MAIN == type)
        {
            overwriteFile(*main_files.entry_rbuf, *mirror_files.entry_wbuf);
            overwriteFile(*main_files.index_rbuf, *mirror_files.index_wbuf);
            mirror_files.ok = true;
        }
        else
        {
            overwriteFile(*mirror_files.entry_rbuf, *main_files.entry_wbuf);
            overwriteFile(*mirror_files.index_rbuf, *main_files.index_wbuf);
            main_files.ok = true;
        }

        LOG_DEBUG(log, "Succeed recovering");
    }
    catch (...)
    {
        status = CORRUPTED;
        throw;
    }
}

void HaMergeTreeLogManager::saveLogfile()
{
    auto date_time = LocalDateTime(time(nullptr));
    std::ostringstream oss;
    oss << '_' << std::setfill('0') << std::setw(2) << UInt32(date_time.month())
        << '.' << std::setfill('0') << std::setw(2) << UInt32(date_time.day())
        << '_' << std::setfill('0') << std::setw(2) << UInt32(date_time.hour())
        << '.' << std::setfill('0') << std::setw(2) << UInt32(date_time.minute())
        << '.' << std::setfill('0') << std::setw(2) << UInt32(date_time.second());
    auto suffix = oss.str();

    try
    {
        Poco::File(log_path + "/log.bin").copyTo(log_path + "/log.bin" + suffix);
        Poco::File(log_path + "/log.idx").copyTo(log_path + "/log.idx" + suffix);
        Poco::File(log_path + "/mirror_log.bin").copyTo(log_path + "/mirror_log.bin" + suffix);
        Poco::File(log_path + "/mirror_log.idx").copyTo(log_path + "/mirror_log.idx" + suffix);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

bool HaMergeTreeLogManager::checkLogDataWithMemory(LogData & data)
{
    if (0 != memcmp(&index_header, &data.header, sizeof(LogIndexFileHeader)))
    {
        return false;
    }
    if (indexes != data.indexes)
    {
        return false;
    }
    if (!std::equal(entries.begin(), entries.end(), data.entries.begin(), data.entries.end(),
        [](const LogEntryPtr & lhs, const LogEntryPtr & rhs)
        {
            return lhs->toString() == rhs->toString();
        }))
    {
        return false;
    }
    return true;
}

void HaMergeTreeLogManager::tryRecover(bool check_log_file_with_mem)
{
    /// Check well log files
    if (main_files.ok)
    {
        LogData main_data = loadDataFromFile(main_files);
        if (check_log_file_with_mem) main_files.ok &= checkLogDataWithMemory(main_data);
    }
    if (mirror_files.ok)
    {
        LogData mirror_data = loadDataFromFile(mirror_files);
        if (check_log_file_with_mem) mirror_files.ok &= checkLogDataWithMemory(mirror_data);
    }

    if (!main_files.ok && mirror_files.ok)
    {
        LOG_WARNING(log, "Main log file has been corrupted, try recovering...");
        recoverFrom(MIRROR);
    }
    else if (main_files.ok && !mirror_files.ok)
    {
        LOG_WARNING(log, "Mirror log file has been corrupted, try overwriting...");
        recoverFrom(MAIN);
    }
    else
    {
        status = CORRUPTED;
        throw Exception("Main and mirror log files are both corrupted which cannot be recovered", ErrorCodes::CORRUPTED_LOG_FILE);
    }
}

void HaMergeTreeLogManager::execWriteAction(
        const char * func,
        const std::function<void(LogFiles & files)> & main_action,
        const std::function<void(LogFiles & files)> & mirror_action,
        const std::function<void()> & check_result)
{
    checkStatus(INTEGRATED, func);

    try
    {
        main_action(main_files);
    }
    catch (...)
    {
        tryLogCurrentException(log, "executing " + toString(func) + "() on main log");

        main_files.ok = false;
        tryRecover(true);
        throw Exception("Failed to write HA log, rolled back to former state", ErrorCodes::CANNOT_WRITE_TO_HA_LOG);
    }

    try
    {
        mirror_action(mirror_files);
        if (check_result) check_result();
    }
    catch (...)
    {
        tryLogCurrentException(log, "executing " + toString(func) + "() on mirror log");

        mirror_files.ok = false;
        tryRecover(false);
    }
}

HaMergeTreeLogManager::LogIndexFileHeader HaMergeTreeLogManager::calcNewIndexHeader(
        const LogIndexFileHeader &header, const std::vector<LogIndex> & indexes)
{
    auto res = header;
    for (auto & index : indexes)
    {
        res.count += 1;
        res.max_lsn = std::max(res.max_lsn, index.lsn);
    }
    return res;
}

void HaMergeTreeLogManager::checkNewIndexes(const IndexVec & lhs, const IndexVec & rhs)
{
    if (lhs != rhs)
        throw Exception("New indexes are inconsistent!", ErrorCodes::INCONSISTENT_LOG_DATA);
}

HaMergeTreeLogManager::IndexVec HaMergeTreeLogManager::writeLogEntriesToFile(
        LogFiles & files,
        const std::vector<LogEntryPtr> & entries,
        const LogIndexFileHeader & header,
        std::lock_guard<std::mutex> &)
{
    auto & index_wbuf = files.index_wbuf;
    auto & entry_wbuf = files.entry_wbuf;
    IndexVec succeed_indexes;

    auto current_offset = entry_wbuf->getPosition();
    /// Write entries
    for (auto & entry : entries)
    {
        auto entry_str = entry->toString();

        LogIndex index {};
        index.lsn = entry->lsn;
        index.offset = current_offset;
        current_offset += entry_str.length();
        index.len = static_cast<UInt32>(entry_str.length());
        succeed_indexes.push_back(index);

        /// Write entry, assumes fp in the end.
        writeString(entry_str, *entry_wbuf);
    }
    entry_wbuf->sync();

    /// Write indexes
    index_wbuf->seek(0, SEEK_SET);
    auto new_header = calcNewIndexHeader(header, succeed_indexes);
    writePODBinary(new_header, *index_wbuf);
    index_wbuf->next();

    index_wbuf->seek(sizeof(LogIndexFileHeader) + sizeof(LogIndex) * header.count, SEEK_SET);
    for (auto & index : succeed_indexes)
        writePODBinary(index, *index_wbuf);
    index_wbuf->sync();

    return succeed_indexes;
}

std::vector<HaMergeTreeLogManager::LogEntryPtr>
HaMergeTreeLogManager::writeLogEntries(std::vector<LogEntryPtr> entries_to_write)
{
    std::lock_guard op_lock(op_mutex);
    /// copy index_header
    auto index_header_copy = index_header;

    /// Make sure the entries are unique and haven't been inserted into here
    auto & unique_entries = entries_to_write;
    auto remove_last = std::remove_if(unique_entries.begin(), unique_entries.end(), [&](auto & e)
    {
        if (e->lsn <= index_header_copy.committed_lsn)
            return true;
        if (auto iter = entries.find(e); iter != entries.end())
            return true;
        return false;
    });

    std::sort(unique_entries.begin(), remove_last, LogEntry::lsn_less_compare);
    auto unique_last = std::unique(unique_entries.begin(), remove_last, LogEntry::lsn_equal_compare);
    unique_entries.erase(unique_last, unique_entries.end());

    if (unique_entries.empty())
        return {};

    /// Begin to write logs
    IndexVec main_indexes;
    IndexVec mirror_indexes;
    auto main_action = [&] (LogFiles & files)
    {
        main_indexes = writeLogEntriesToFile(files, unique_entries, index_header_copy, op_lock);
    };
    auto mirror_action = [&] (LogFiles & files)
    {
        mirror_indexes = writeLogEntriesToFile(files, unique_entries, index_header_copy, op_lock);
    };
    auto check_result = [&] { checkNewIndexes(main_indexes, mirror_indexes); };

    execWriteAction(__func__, main_action, mirror_action, check_result);

    std::lock_guard mem_lock(mem_mutex);
    /// Copy the succeed entries
    index_header = calcNewIndexHeader(index_header_copy, main_indexes);
    indexes.insert(main_indexes.begin(), main_indexes.end());
    for (auto & entry : unique_entries)
    {
        entries.insert(entry);

        /*
        if (entry->transaction_id != INVALID_XID && entry->type == LogEntry::GET_PART &&
         !entry->is_executed)
        {
            TransactionManager & transactionManager = storage.global_context.getTransactionManager();
            auto transaction = transactionManager.constructTransaction(entry->transaction_id);
            auto transaction_item_ptr = std::make_shared<MergeTreeTransactionItem>(storage, entry->transaction_index,
                                                                                   entry->new_part_name,
                                                                                   entry->transaction_id, IN_PROGRESS, entry->lsn);
            transaction->attachItem(transaction_item_ptr);
        }
        */
    }

    return unique_entries;
}

bool HaMergeTreeLogManager::writeLogEntry(const LogEntryPtr & entry)
{
    auto succeed_entries = writeLogEntries({entry});
    return !succeed_entries.empty();
}

bool HaMergeTreeLogManager::writeLogEntry(const LogEntry & entry_to_write)
{
    auto succeed_entries = writeLogEntries({std::make_shared<LogEntry>(entry_to_write)});
    return !succeed_entries.empty();
}

void HaMergeTreeLogManager::markLogEntryExecuted(UInt64 lsn)
{
    markLogEntriesExecuted(std::vector<UInt64>{lsn});
}

void HaMergeTreeLogManager::markLogEntriesExecuted(const std::vector<UInt64> & lsns)
{
    std::lock_guard op_lock(op_mutex);

    /// Copy the entries. Make sure the raw entry not modified before IO finished
    /// Record the offsets in log file
    std::vector<std::pair<LogEntry, UInt64>> copy_entries;
    copy_entries.reserve(lsns.size());

    for (auto lsn : lsns)
    {
        auto entry_iter = entries.find(lsn);
        auto index_iter = indexes.find(lsn);
        if (entry_iter == entries.end() || index_iter == indexes.end())
        {
            LOG_ERROR(log, "Log entry NOT FOUND: {}", lsn);
            continue;
        }

        if ((*entry_iter)->is_executed)
            continue;

        copy_entries.emplace_back(**entry_iter, index_iter->offset);
        copy_entries.back().first.is_executed = true;
    }

    auto write_executed = [&copy_entries](LogFiles & files)
    {
        for (auto & [entry, offset] : copy_entries)
        {
            files.entry_wbuf->seek(offset, SEEK_SET);
            entry.writeText(*files.entry_wbuf);
            files.entry_wbuf->next();
        }
        files.entry_wbuf->sync();
        /// Make sure the fp is always END after used
        files.entry_wbuf->seek(0, SEEK_END);
    };

    execWriteAction(__func__, write_executed, write_executed);

    std::lock_guard mem_lock(mem_mutex);
    for (auto & p : copy_entries)
        (**entries.find(p.first.lsn)).is_executed = true;
}

void HaMergeTreeLogManager::markLogEntriesExecuted(const LogEntry::Vec & executed_entries)
{
    std::vector<UInt64> lsns;
    lsns.reserve(executed_entries.size());
    bool need_update_time = false;
    for (auto & entry : executed_entries)
    {
        lsns.push_back(entry->lsn);
        /// If there is data changes, we need to update the table's update_time
        if (entry->mayChangeStorageData())
            need_update_time = true;

        if (entry->type == LogEntry::GET_PART || entry->type == LogEntry::DROP_RANGE ||
            entry->type == LogEntry::CLONE_PART || entry->type == LogEntry::CLEAR_COLUMN)
        {
            /// MergeTreePartInfo part_info;
            /// MergeTreePartInfo::tryParsePartName(entry->new_part_name, &part_info, storage.format_version);
            /// partition_ids.insert(part_info.partition_id);

            if (entry->type == LogEntry::GET_PART || entry->type == LogEntry::CLONE_PART)
            {
                /// TODO: storage.collectMapKeyForPartitions(partition_ids);
            }
            else if (entry->type == LogEntry::DROP_RANGE)
            {
                /// auto it = std::max_element(partition_ids.begin(), partition_ids.end());
                /// if (it != partition_ids.end())
                /// TODO:   storage.recalculateMapKeyCacheByMaxPartition(*it);
            }
        }
        else if (entry->type == LogEntry::REPLACE_RANGE)
        {
            /// TODO:
        }
    }
    markLogEntriesExecuted(lsns);
    if (need_update_time)
        storage.setUpdateTimeNow();
}

UInt64 HaMergeTreeLogManager::calcUpdatedLSNUnsafe()
{
    LogEntryList::const_iterator next;

    if (cached_updated_lsn == index_header.committed_lsn)
    {
        next = entries.begin(); /// Usually after creating or commiting
    }
    else
    {
        if (cached_updated_iter == entries.end())
        {
            LOG_WARNING(
                log,
                "Logical error: cached_updated_iter should be valid: cached_updated_lsn {}, comitted_lsn {}",
                cached_updated_lsn,
                index_header.committed_lsn);
            next = entries.begin();
        }
        else
        {
            next = std::next(cached_updated_iter);
        }
    }

    while (next != entries.end())
    {
        auto & e = *next;

        if (e->lsn <= index_header.committed_lsn)
        {
            LOG_WARNING(log, "Find a entry of which lsn {}  <= committed_lsn: {}", e->lsn, index_header.committed_lsn);
            cached_updated_iter = next++;
            continue;
        }

        /// First not executed log entry
        if (!e->is_executed)
            break;

        /// Got a hole
        if (e->lsn - cached_updated_lsn > 1)
        {
            LOG_TRACE(log, "There is a hole between the log entries: {} ~ {}", cached_updated_lsn, e->lsn);
            break;
        }

        /// Move on
        cached_updated_lsn = e->lsn;
        cached_updated_iter = next++;
    }

    return cached_updated_lsn;
}

UInt64 HaMergeTreeLogManager::calcMaxExecutedLSN()
{
    std::shared_lock mem_lock(mem_mutex);

    for (auto iter = entries.rbegin(); iter != entries.rend(); ++iter)
    {
        if ((*iter)->is_executed)
            return (*iter)->lsn;
    }
    return index_header.committed_lsn;
}

void HaMergeTreeLogManager::commitTo(UInt64 lsn_to_commit)
{
    std::lock_guard op_lock(op_mutex);

    /// if (!storage.global_context.getTransactionManager().empty())
    /// {
    ///     return;
    /// }

    if (lsn_to_commit <= index_header.committed_lsn)
        throw Exception("Try to commit a smaller/equal LSN " + toString(lsn_to_commit)
                + ", current committed_lsn " + toString(index_header.committed_lsn),
                ErrorCodes::LOGICAL_ERROR);

    auto new_header = index_header;
    new_header.committed_lsn = lsn_to_commit;

    auto write_header = [&new_header] (LogFiles & files)
    {
        files.index_wbuf->seek(0, SEEK_SET);
        writePODBinary(new_header, *files.index_wbuf);
        files.index_wbuf->sync();
    };

    execWriteAction(__func__, write_header, write_header);

    std::lock_guard mem_lock(mem_mutex);
    index_header = new_header;

    entries.erase(entries.begin(), entries.upper_bound(lsn_to_commit));
    indexes.erase(indexes.begin(), indexes.upper_bound(lsn_to_commit));

    if (lsn_to_commit >= cached_updated_lsn)
    {
        /// reset updated_lsn state
        cached_updated_lsn = lsn_to_commit;
        cached_updated_iter = entries.end();
    }
}

HaMergeTreeLogManager::IndexVec HaMergeTreeLogManager::rewriteLogEntriesToFile(
        LogFiles & files,
        const std::vector<LogEntryPtr> & entries,
        const LogIndexFileHeader & header,
        std::lock_guard<std::mutex> & discard_lock)
{
    /// Reset state
    files.index_wbuf->truncate(0);
    files.entry_wbuf->truncate(0);
    files.index_wbuf->sync();
    files.entry_wbuf->sync();

    files.entry_wbuf->seek(0, SEEK_SET);
    return writeLogEntriesToFile(files, entries, header, discard_lock);
}

void HaMergeTreeLogManager::discardTo(UInt64 lsn_to_discard)
{
    std::lock_guard op_lock(op_mutex);

    if (lsn_to_discard > index_header.committed_lsn)
        throw Exception("Try to discard an uncommitted LSN: " + toString(lsn_to_discard), ErrorCodes::LOGICAL_ERROR);

    /// Prepare new header
    auto new_header = index_header;
    /// set count to zero due to rewriting all the entries
    new_header.count = 0;
    /// Copy current entries
    std::vector<LogEntryPtr> copied_entries(entries.begin(), entries.end());

    IndexVec main_indexes;
    IndexVec mirror_indexes;
    auto main_action = [&] (LogFiles & files)
    {
        main_indexes = rewriteLogEntriesToFile(files, copied_entries, new_header, op_lock);
    };
    auto mirror_action = [&] (LogFiles & files)
    {
        mirror_indexes = rewriteLogEntriesToFile(files, copied_entries, new_header, op_lock);
    };
    auto check_result = [&] { checkNewIndexes(main_indexes, mirror_indexes); };

    execWriteAction(__func__, main_action, mirror_action, check_result);

    std::lock_guard mem_lock(mem_mutex);
    /// Update header, indexes
    index_header = calcNewIndexHeader(new_header, main_indexes);
    indexes.clear();
    indexes.insert(main_indexes.begin(), main_indexes.end());
}

void HaMergeTreeLogManager::resetTo(UInt64 lsn_to_commit)
{
    std::lock_guard op_lock(op_mutex);

    auto new_header = index_header;
    new_header.count = 0;
    new_header.committed_lsn = lsn_to_commit;
    new_header.max_lsn = lsn_to_commit;

    auto action = [&] (LogFiles & files)
    {
        rewriteLogEntriesToFile(files, {}, new_header, op_lock);
    };

    execWriteAction(__func__, action, action);

    std::lock_guard mem_lock(mem_mutex);
    index_header = new_header;
    entries.clear();
    indexes.clear();

    /// reset updated_lsn state
    cached_updated_lsn = lsn_to_commit;
    cached_updated_iter = entries.end();
}

/*
bool HaMergeTreeLogManager::checkTransactionLog(const TransactionID & xid, const HaMergeTreeLogEntry::Type & type,
                         const TransactionStatus & tran_status)
{
    std::shared_lock check_lock(mem_mutex);
    return std::any_of(entries.begin(), entries.end(), [&](const LogEntryPtr & log_entry_ptr){
        return log_entry_ptr->transaction_id == xid && log_entry_ptr->type == type &&
        log_entry_ptr->transaction_status == tran_status;
    });
}

bool HaMergeTreeLogManager::completeTransaction(const TransactionID & xid, const HaMergeTreeLogEntry::Type & type,
        UInt64 terminate_item_index)
{
    std::shared_lock check_lock(mem_mutex);
    UInt64 sum_index = 0;
    for (auto & log_entry_ptr : entries)
    {
        if (log_entry_ptr->transaction_id == xid && log_entry_ptr->type == type
        && log_entry_ptr->is_executed)
        {
            sum_index += log_entry_ptr->transaction_index;
        }
    }

    return sum_index == terminate_item_index;
}

bool HaMergeTreeLogManager::verifyPartCanMerged(const Strings & parts_need_merged)
{
    std::shared_lock check_lock(mem_mutex);
    for(auto & entry : entries)
    {
        if (entry->transaction_id != INVALID_XID && entry->type == LogEntry::GET_PART
        && (entry->transaction_status == IN_PROGRESS || entry->transaction_status == SUB_COMMITTED))
        {
            if (parts_need_merged.end() != std::find(parts_need_merged.begin(),
                    parts_need_merged.end(), entry->new_part_name))
            {
                LOG_INFO(log, "Log entry with valid transaction-" << entry->toString() << " Cancel merge action from parts:" << "\n");
                for (const auto & part_name: parts_need_merged)
                    LOG_INFO(log, part_name);
                return false;
            }
        }
    }
    return true;
}

void HaMergeTreeLogManager::updateLogTransaction(UInt64 lsn, TransactionStatus tran_status)
{
    std::lock_guard mark_lock(op_mutex);
    auto entry_iter = entries.find(lsn);
    auto index_iter = indexes.find(lsn);
    if (entry_iter == entries.end() || index_iter == indexes.end())
    {
        throw Exception("Log entry NOT FOUND: " + toString(lsn), ErrorCodes::LOGICAL_ERROR);
    }

    /// Copy the entry. Make sure the raw entry not modified before IO finished
    auto e = **entry_iter;
    e.transaction_status = tran_status;

    auto write_mark = [&e, &index_iter] (LogFiles & files)
    {
        files.entry_wbuf->seek(index_iter->offset);
        e.writeText(*files.entry_wbuf);
        files.entry_wbuf->sync();

        /// move fp back
        files.entry_wbuf->seek(0, SEEK_END);
    };

    execWriteAction(__func__, write_mark, write_mark);

    std::lock_guard mem_lock(mem_mutex);
    (*entry_iter)->transaction_status = tran_status;
}
*/

} // end of namespace DB
