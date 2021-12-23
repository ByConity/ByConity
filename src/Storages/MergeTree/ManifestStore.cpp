#include <Storages/MergeTree/ManifestStore.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <common/logger_useful.h>
#include <iomanip>
#include <sstream>

namespace DB
{

String ManifestLogEntry::toText() const
{
    WriteBufferFromOwnString buf;
    writeText(buf);
    return buf.str();
}

void ManifestLogEntry::sanityCheck() const
{
    if (prev_version >= version)
        throw Exception("Invalid manifest log, prev_version >= version: \n" + toText(), ErrorCodes::LOGICAL_ERROR);
    if (source_replica.empty())
        throw Exception("Invalid manifest log, source replica not set: \n" + toText(), ErrorCodes::LOGICAL_ERROR);
    if (type == SYNC_PARTS && added_parts.empty() && updated_parts.empty() && removed_parts.empty())
        throw Exception("Invalid manifest log, missing fields: \n" + toText(), ErrorCodes::LOGICAL_ERROR);
    if (type == DETACH_PARTS && removed_parts.empty())
        throw Exception("Invalid manifest log, missing fields: \n" + toText(), ErrorCodes::LOGICAL_ERROR);
    if (type == RESET_MANIFEST && prev_version != target_version)
        throw Exception("Invalid manifest log, prev_version and target_version not equal for reset manifest log: \n" + toText(), ErrorCodes::LOGICAL_ERROR);
    if ((type == RESET_MANIFEST || type == CHECKPOINT_MANIFEST) && target_version == 0)
        throw Exception("Invalid manifest log, missing target_version: \n" + toText(), ErrorCodes::LOGICAL_ERROR);
    if (type == MODIFY_SCHEMA && (metadata_str.empty() || columns_str.empty()))
        throw Exception("Invalid manifest log, missing fields: \n" + toText(), ErrorCodes::LOGICAL_ERROR);
}

void ManifestLogEntry::checkConsecutiveLogs(const std::vector<ManifestLogEntry> & entries)
{
    entries.front().sanityCheck();
    for (size_t i = 1; i < entries.size(); ++i)
    {
        entries[i].sanityCheck();
        if (entries[i - 1].version != entries[i].prev_version)
            throw Exception("Invalid manifest logs: prev.version != next.prev_version", ErrorCodes::LOGICAL_ERROR);
        if (entries[i - 1].version >= entries[i].version)
            throw Exception("Invalid manifest logs: prev.version >= next.version", ErrorCodes::LOGICAL_ERROR);
    }
}

static void writeLabeledNames(WriteBuffer & out, const char * label, const Strings & names)
{
    out << label << names.size() << "\n";
    for (auto & name : names)
        out << name << "\n";
}

static void readLabeledNames(ReadBuffer & in, const char * label, Strings & names)
{
    size_t num;
    in >> label >> num >> "\n";
    names.resize(num);
    for (size_t i = 0; i < num; ++i)
        in >> names[i] >> "\n";
}

void ManifestLogEntry::writeText(WriteBuffer & out) const
{
    out << "format:" << format_version << "\n";
    out << "version:" << version << "\n";
    out << "prev_version:" << prev_version << "\n";
    out << "create_time:" << LocalDateTime(create_time ? create_time : time(nullptr)) << "\n";
    out << "source_replica:" << source_replica << "\n";

    switch (type)
    {
        case SYNC_PARTS:
            out << "sync_parts\n";
            writeLabeledNames(out, "added:", added_parts);
            writeLabeledNames(out, "updated:", updated_parts);
            writeLabeledNames(out, "removed:", removed_parts);
            break;
        case DETACH_PARTS:
            out << "detach_parts\n";
            writeLabeledNames(out, "removed:", removed_parts);
            break;
        case RESET_MANIFEST:
            out << "reset_manifest\n";
            out << "target_version:" << target_version << "\n";
            break;
        case CHECKPOINT_MANIFEST:
            out << "checkpoint_manifest\n";
            out << "target_version:" << target_version << "\n";
            break;
        case MODIFY_SCHEMA:
            out << "modify_schema\n";
            out << "metadata:";
            writeDoubleQuotedString(metadata_str, out);
            out << "columns:";
            writeDoubleQuotedString(columns_str, out);
            break;
        default:
            throw Exception("Unknown manifest log type: " + toString<int>(type), ErrorCodes::LOGICAL_ERROR);
    }
    out << "\n";
}

void ManifestLogEntry::readText(ReadBuffer & in)
{
    UInt8 format;
    in >> "format:" >> format >> "\n";
    if (format != format_version)
        throw Exception("unknown manifest format version : " + toString(format), ErrorCodes::LOGICAL_ERROR);

    LocalDateTime create_time_dt;
    in >> "version:" >> version >> "\n";
    in >> "prev_version:" >> prev_version >> "\n";
    in >> "create_time:" >> create_time_dt >> "\n";
    create_time = create_time_dt;
    in >> "source_replica:" >> source_replica >> "\n";

    String type_str;
    in >> type_str >> "\n";
    if (type_str == "sync_parts")
    {
        type = SYNC_PARTS;
        readLabeledNames(in, "added:", added_parts);
        readLabeledNames(in, "updated:", updated_parts);
        readLabeledNames(in, "removed:", removed_parts);
    }
    else if (type_str == "detach_parts")
    {
        type = DETACH_PARTS;
        readLabeledNames(in, "removed:", removed_parts);
    }
    else if (type_str == "reset_manifest")
    {
        type = RESET_MANIFEST;
        in >> "target_version:" >> target_version >> "\n";
    }
    else if (type_str == "checkpoint_manifest")
    {
        type = CHECKPOINT_MANIFEST;
        in >> "target_version:" >> target_version >> "\n";
    }
    else if (type_str == "modify_schema")
    {
        type = MODIFY_SCHEMA;
        in >> "metadata:";
        readDoubleQuotedString(metadata_str, in);
        in >> "columns:";
        readDoubleQuotedString(columns_str, in);
    }
    else
    {
        throw Exception("Unknown manifest log type: " + type_str, ErrorCodes::LOGICAL_ERROR);
    }
    in >> "\n";
}

ManifestStore::ManifestStore(String dir_, const String & logger_name)
    : dir(std::move(dir_)), logger(nullptr), db(nullptr), time_of_last_commit_log{0},
      latest_version(0), commit_version(0), checkpoint_version(0)
{
    logger = &Poco::Logger::get(logger_name);
}

ManifestStore::~ManifestStore()
{
    close();
}

void ManifestStore::open(const String& metadata, const String& columns)
{

    if (db != nullptr)
        return;

    if (metadata.empty() || columns.empty())
        throw Exception("Expected table schema metadata or columns not empty", ErrorCodes::LOGICAL_ERROR);

    LOG_TRACE(logger, "Opening the manifest store...");

    auto checkpoint_lock = checkpointLock();
    auto write_lock = writeLock();

    // TODO configure block cache (default is 8MB per db)
    rocksdb::Options options;
    String db_dir = dir + "/db";
    auto status = rocksdb::DB::Open(options, db_dir, &db);
    if (status.ok())
    {
        /// successfully opened exist db, load version info
        auto keys = {
            rocksdb::Slice(latest_version_key),
            rocksdb::Slice(commit_version_key),
            rocksdb::Slice(checkpoint_version_key)
        };
        Strings values(3);
        auto result = db->MultiGet(rocksdb::ReadOptions(), keys, &values);
        for (auto & st : result)
        {
            if (!st.ok())
                throw Exception("Can't read version from manifest DB: " + st.ToString(), ErrorCodes::MANIFEST_OPERATION_ERROR);
        }
        latest_version.store(parse<Version>(values[0]), std::memory_order_release);
        commit_version.store(parse<Version>(values[1]), std::memory_order_release);
        checkpoint_version.store(parse<Version>(values[2]), std::memory_order_release);

        /// get create time of last commit log entry if exists
        if (commit_version > 0)
        {
            ManifestLogEntry last_commit_entry;
            if (!containsLog(commit_version, &last_commit_entry))
                throw Exception("Can't load last committed log at " + toString(commit_version.load(std::memory_order_relaxed)) + ": log not found",
                                ErrorCodes::MANIFEST_OPERATION_ERROR);
            time_of_last_commit_log.store(last_commit_entry.create_time, std::memory_order_relaxed);
        }

        LOG_DEBUG(logger, "Successfully opened the manifest store, with latest_version={}, commit_version={}, checkpoint_version={}, time_of_last_commit_log={} ", latest_version
                      , commit_version
                      , checkpoint_version
                      , time_of_last_commit_log);
    }
    else
    {
        LOG_DEBUG(logger, "Manifest store does not exist, creating a new one.");

        /// db doesn't exist, create a new one
        options.create_if_missing = true;
        options.error_if_exists = true;
        status = rocksdb::DB::Open(options, db_dir, &db);
        if (!status.ok())
            throw Exception("Can't create manifest DB at " + db_dir + ": " + status.ToString(), ErrorCodes::MANIFEST_OPERATION_ERROR);

        /// write version info
        latest_version = commit_version = checkpoint_version = 0;
        rocksdb::WriteOptions wopt;
        wopt.sync = true;
        rocksdb::WriteBatch updates;
        updates.Put(latest_version_key, "0");
        updates.Put(commit_version_key, "0");
        updates.Put(checkpoint_version_key, "0");
        status = db->Write(wopt, &updates);
        if (!status.ok())
            throw Exception("Can't write initial version to manifest: " + status.ToString(), ErrorCodes::MANIFEST_OPERATION_ERROR);
    }

    if (!lookForLatestTableSchema())
    {
        LOG_INFO(logger, "Upgrading manifest, adding schema keys and files...");

        /// If we DID NOT found the schema, write the current schema into rocksdb.
        rocksdb::WriteBatch updates;
        updates.Put(latest_metadata_key, metadata);
        updates.Put(latest_columns_key, columns);

        rocksdb::WriteOptions opt;
        opt.sync = true;
        status = db->Write(opt, &updates);
        if (!status.ok())
        {
            throw Exception("Failed to write table schema", ErrorCodes::MANIFEST_OPERATION_ERROR);
        }

        metadata_str = metadata;
        columns_str = columns;

        LOG_INFO(logger, "[1/3] successfully adding schema keys. metadata:\n {} \ncolumns:\n {}", metadata_str, columns_str);

        /// If already has a checkpoint file, we also need to add schema files.
        if (checkpointVersion() > 0)
        {
            // Save metadata to file
            String metadata_path = metadataFilePath(checkpointVersion());
            WriteBufferFromFile metadata_buf(metadata_path, 4096);
            writeString(metadata_str, metadata_buf);
            metadata_buf.sync();
            LOG_INFO(logger, "[2/3] wrote checkpoint's metadata file {}", metadata_path);

            // Save columns to file
            String columns_path = columnsFilePath(checkpointVersion());
            WriteBufferFromFile columns_buf(columns_path, 4096);
            writeString(columns_str, columns_buf);
            columns_buf.sync();
            LOG_INFO(logger, "[3/3] wrote checkpoint's columns file {}", columns_path);
        }
        else
        {
            LOG_INFO(logger, "[3/3] no checkpoint, skip adding schema files");
        }
    }

    LOG_TRACE(logger, "manifest store is opened");

    // Guarantee version invariant holds.
    checkVersionInvariant();
}

void ManifestStore::close()
{
    auto checkpoint_lock = checkpointLock();
    auto write_lock = writeLock();
    if (db != nullptr)
    {
        LOG_TRACE(logger, "Closing manifest store");
        auto status = db->Close();
        if (!status.ok())
        {
            LOG_WARNING(logger, "Failed to close manifest DB: {}", status.ToString());
        }
        delete db;
        db = nullptr;
    }
}

void ManifestStore::append(const WriteLock &, const ManifestStore::LogEntries & entries, bool commit)
{
    if (entries.empty())
        return;

    checkOpened();

    /// First guarantee version invariant holds.
    checkVersionInvariant();

    /// Then guarantee the logs will be appended are consecutive internally and also valid.
    ManifestLogEntry::checkConsecutiveLogs(entries);

    if (entries.front().prev_version != latestVersion())
        throw Exception("manifest new log's prev_version " + toString(entries.front().prev_version) + " doesn't match latest version " + toString(latestVersion()), ErrorCodes::LOGICAL_ERROR);

    if (commit && latestVersion() != commitVersion())
        throw Exception("append and commit log requires manifest's commit version equal to latest version, but current state is " + stateSummary(), ErrorCodes::LOGICAL_ERROR);

    Version new_latest_version = entries.back().version;
    String new_latest_metadata;
    String new_latest_columns;
    for (auto & entry : entries)
    {
        if (entry.type == ManifestLogEntry::MODIFY_SCHEMA)
        {
            new_latest_metadata = entry.metadata_str;
            new_latest_columns = entry.columns_str;
        }
    }

    rocksdb::WriteBatch updates;
    for (auto & entry : entries)
        updates.Put(generateLogKey(entry.version), entry.toText());

    updates.Put(latest_version_key, toString(new_latest_version));

    if (commit)
    {
        updates.Put(commit_version_key, toString(new_latest_version));
        /// Write the latest table schema if any.
        if (!new_latest_metadata.empty() && !new_latest_columns.empty())
        {
            updates.Put(latest_metadata_key, new_latest_metadata);
            updates.Put(latest_columns_key, new_latest_columns);
        }

        if (entries.back().type == ManifestLogEntry::RESET_MANIFEST)
        {
            auto snapshot = getSnapshot(entries.back().version);
            updates.Put(latest_metadata_key, snapshot.metadata_str);
            updates.Put(latest_columns_key, snapshot.columns_str);
        }
    }

    rocksdb::WriteOptions options;
    options.sync = true;
    auto status = db->Write(options, &updates);
    if (!status.ok())
        throw Exception("Failed to update manifest latest version to " + toString(new_latest_version) + ": " + status.ToString(), ErrorCodes::MANIFEST_OPERATION_ERROR);

    latest_version.store(new_latest_version, std::memory_order_release);
    if (commit)
    {
        if (!new_latest_metadata.empty() && !new_latest_columns.empty())
        {
            metadata_str = new_latest_metadata;
            columns_str = new_latest_columns;
        }

        if (entries.back().type == ManifestLogEntry::RESET_MANIFEST)
        {
            auto snapshot = getSnapshot(entries.back().version);
            metadata_str = snapshot.metadata_str;
            columns_str = snapshot.columns_str;
        }

        commit_version.store(new_latest_version, std::memory_order_release);
        time_of_last_commit_log.store(entries.back().create_time, std::memory_order_relaxed);
        for (auto & entry : entries)
            putPartRemovedVersion(entry.removed_parts, entry.version);
        LOG_DEBUG(logger, "Updated manifest commit version to {} during append", new_latest_version);
    }

    /// Guarantee version invariant still holds after we done appending.
    checkVersionInvariant();
}

ManifestStore::Snapshot ManifestStore::getSnapshot(Version version) const
{
    checkOpened();
    checkVersionIsValid(version);
    if (version < checkpointVersion())
        throw Exception("Can't get snapshot at " + toString(version) + " because it's older than last checkpoint " + toString(checkpointVersion()), ErrorCodes::LOGICAL_ERROR);
    Snapshot result;
    if (version == 0)
    {
        result.metadata_str = metadata_str;
        result.columns_str = columns_str;
        return result;
    }
    auto lock = checkpointLock();
    return getSnapshotImpl(lock, version);
}

ManifestStore::LogEntries ManifestStore::getLogEntries(Version from, size_t limit) const
{
    checkOpened();
    checkVersionIsValid(from);
    if (from < checkpointVersion())
        throw Exception("Can't get manifest logs from " + toString(from) + " because it's smaller than checkpoint version " + toString(checkpointVersion()), ErrorCodes::LOGICAL_ERROR);
    if (from > latestVersion())
        return {};
    LogEntries res;
    String start_key = generateLogKey(from);
    String end_key = generateLogKey(latestVersion() + 1);
    rocksdb::ReadOptions opt;
    rocksdb::Slice upper_bound(end_key);
    opt.iterate_upper_bound = &upper_bound;
    std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(opt));
    for (iter->Seek(start_key); iter->Valid() && (limit == 0 || res.size() < limit); iter->Next())
    {
        ReadBufferFromMemory in(iter->value().data(), iter->value().size());
        ManifestLogEntry value;
        value.readText(in);
        res.emplace_back(value);
    }
    if (!iter->status().ok())
        throw Exception("Failed to read manifest in [" + start_key + "," + end_key + ") : " + iter->status().ToString(), ErrorCodes::MANIFEST_OPERATION_ERROR);
    return res;
}

bool ManifestStore::containsLog(ManifestStore::Version version, ManifestLogEntry * entry) const
{
    checkOpened();
    checkVersionIsValid(version);
    if (version > latestVersion())
        return false;
    String key(generateLogKey(version));
    String value;
    auto status = db->Get(rocksdb::ReadOptions(), key, &value);
    if (status.ok())
    {
        if (entry)
        {
            ReadBufferFromMemory in(value.data(), value.size());
            entry->readText(in);
        }
        return true;
    }
    if (status.IsNotFound())
        return false;
    throw Exception("Failed to get manifest log at " + key + ": " + status.ToString(), ErrorCodes::MANIFEST_OPERATION_ERROR);
}

inline String ManifestStore::generateLogKey(Version version)
{
    checkVersionIsValid(version);
    std::ostringstream ss;
    ss << log_key_prefix << std::setfill('0') << std::setw(10) << version;
    return ss.str();
}

inline String ManifestStore::formatFilePath(String path, Version version) const
{
    std::ostringstream ss;
    ss << dir << "/" << path << "." << std::setfill('0') << std::setw(10) << version;
    return ss.str();
}

inline String ManifestStore::checkpointFilePath(Version version) const
{
    return formatFilePath("checkpoint", version);
}

inline String ManifestStore::metadataFilePath(Version version) const
{
    return formatFilePath("metadata", version);
}

inline String ManifestStore::columnsFilePath(Version version) const
{
    return formatFilePath("columns", version);
}

void ManifestStore::checkpointTo(Version version)
{
    checkOpened();
    checkVersionIsValid(version);
    checkVersionInvariant();

    auto lock = checkpointLock();
    if (version <= checkpointVersion())
    {
        LOG_WARNING(logger, "Request to checkpoint an older version {} ,last checkpoint version is {}", version, checkpointVersion());
        return;
    }

    String start_key;
    String end_key;
    Snapshot snapshot = getSnapshotImpl(lock, version, &start_key, &end_key);
    writeCheckpointFile(version, snapshot);

    rocksdb::WriteBatch updates;
    updates.Put(checkpoint_version_key, toString(version));
    updates.DeleteRange(start_key, end_key);

    rocksdb::WriteOptions opt;
    opt.sync = true;
    auto status = db->Write(opt, &updates);
    if (!status.ok())
        throw Exception("Failed to update manifest checkpoint version to " + toString(version), ErrorCodes::MANIFEST_OPERATION_ERROR);

    Version old_version = checkpointVersion();
    checkpoint_version.store(version, std::memory_order_release);
    String old_path = checkpointFilePath(old_version);
    /// there is no checkpoint file for version 0
    if (old_version != 0)
    {
        Poco::File old_file(old_path);
        old_file.remove();
        Poco::File old_metadata_file(metadataFilePath(old_version));
        old_metadata_file.remove();
        Poco::File old_columns_file(columnsFilePath(old_version));
        old_columns_file.remove();
    }
    LOG_INFO(
        logger,
        "Updated manifest checkpoint version from {} to {} with {} parts in total", old_version, version, snapshot.parts.size());

    // Guarantee version invariant still holds.
    checkVersionInvariant();
}

void ManifestStore::commitLog(const WriteLock &, Version version)
{
    checkOpened();
    checkVersionIsValid(version);
    checkVersionInvariant();

    ManifestLogEntry entry;
    if (!containsLog(version, &entry))
        throw Exception("Can't commit log at " + toString(version) + ": log not found", ErrorCodes::LOGICAL_ERROR);
    if (entry.prev_version != commitVersion())
        throw Exception("Can't commit log at " + toString(version) + ": not all previous logs are committed, manifest state: " + stateSummary(), ErrorCodes::LOGICAL_ERROR);

    rocksdb::WriteBatch updates;
    updates.Put(commit_version_key, toString(version));
    if (entry.type == ManifestLogEntry::MODIFY_SCHEMA)
    {
        /// Write the latest table schema.
        updates.Put(latest_metadata_key, entry.metadata_str);
        updates.Put(latest_columns_key, entry.columns_str);
    }
    else if (entry.type == ManifestLogEntry::RESET_MANIFEST)
    {
        auto snapshot = getSnapshot(version);
        /// Write the latest table schema
        updates.Put(latest_metadata_key, snapshot.metadata_str);
        updates.Put(latest_columns_key, snapshot.columns_str);
    }

    rocksdb::WriteOptions opt;
    opt.sync = true;
    auto status = db->Write(opt, &updates);
    if (!status.ok())
        throw Exception("Failed to commit manifest to " + toString(version) + ": " + status.ToString(), ErrorCodes::MANIFEST_OPERATION_ERROR);

    if (entry.type == ManifestLogEntry::MODIFY_SCHEMA)
    {
        metadata_str = entry.metadata_str;
        columns_str = entry.columns_str;
    }
    else if (entry.type == ManifestLogEntry::RESET_MANIFEST)
    {
        auto snapshot = getSnapshot(version);
        metadata_str = snapshot.metadata_str;
        columns_str = snapshot.columns_str;
    }
    commit_version.store(version, std::memory_order_release);
    time_of_last_commit_log.store(entry.create_time, std::memory_order_relaxed);
    putPartRemovedVersion(entry.removed_parts, entry.version);
    LOG_DEBUG(logger, "Updated manifest commit version to {}", version);

    // Guarantee version invariant still holds.
    checkVersionInvariant();
}

ManifestStore::Snapshot ManifestStore::getSnapshotImpl(
    const CheckpointLock &,
    Version version,
    String * delta_start_key,
    String * delta_end_key) const
{
    Snapshot snapshot = readCheckpointFile(checkpointVersion());

    if (snapshot.metadata_str.empty() || snapshot.columns_str.empty())
    {
        throw Exception("Expected table schema metadata and columns exist", ErrorCodes::LOGICAL_ERROR);
    }

    /// merge deltas [checkpoint_version + 1, version + 1) into snapshot
    String start_key = generateLogKey(checkpointVersion() + 1);
    String end_key = generateLogKey(version + 1);
    rocksdb::ReadOptions opt;
    rocksdb::Slice upper_bound(end_key);
    opt.iterate_upper_bound = &upper_bound;
    std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(opt));
    for (iter->Seek(start_key); iter->Valid(); iter->Next())
    {
        ReadBufferFromMemory in(iter->value().data(), iter->value().size());
        ManifestLogEntry value;
        value.readText(in);
        if (value.type == ManifestLogEntry::SYNC_PARTS)
        {
            for (auto & part : value.added_parts)
                snapshot.parts[part] = value.version;
            for (auto & part : value.updated_parts)
                snapshot.parts[part] = value.version;
            for (auto & part : value.removed_parts)
                snapshot.parts.erase(part);
        }
        if (value.type == ManifestLogEntry::DETACH_PARTS)
        {
            for (auto & part : value.removed_parts)
                snapshot.parts.erase(part);
        }
        if (value.type == ManifestLogEntry::MODIFY_SCHEMA)
        {
            snapshot.metadata_str = value.metadata_str;
            snapshot.columns_str = value.columns_str;
        }
    }
    if (!iter->status().ok())
        throw Exception("Failed to read manifest in [" + start_key + "," + end_key + ") : " + iter->status().ToString(), ErrorCodes::MANIFEST_OPERATION_ERROR);

    if (delta_start_key)
        *delta_start_key = start_key;
    if (delta_end_key)
        *delta_end_key = end_key;
    return snapshot;
}

void ManifestStore::writeCheckpointFile(Version version, const ManifestStore::Snapshot & snapshot) const
{
    if (snapshot.columns_str.empty() || snapshot.metadata_str.empty())
        throw Exception(
            "Without schema found in snapshot, failed to write checkpoint file at version: " + toString(version),
            ErrorCodes::LOGICAL_ERROR);

    // Write part delete version
    String path = checkpointFilePath(version);
    WriteBufferFromFile buf(path, 4096);
    for (auto & entry : snapshot.parts)
    {
        writeString(entry.first, buf);
        writeString("\t", buf);
        writeIntText(entry.second, buf);
        writeString("\n", buf);
    }
    buf.sync();

    // Write table schema metadata
    String metadata_path = metadataFilePath(version);
    WriteBufferFromFile metadata_buf(metadata_path, 4096);
    writeString(snapshot.metadata_str, metadata_buf);
    metadata_buf.sync();

    // Write table schema columns
    String columns_path = columnsFilePath(version);
    WriteBufferFromFile columns_buf(columns_path, 4096);
    writeString(snapshot.columns_str, columns_buf);
    columns_buf.sync();
}

ManifestStore::Snapshot ManifestStore::readCheckpointFile(Version version) const
{
    Snapshot result;
    if (version == 0)
    {
        result.metadata_str = metadata_str;
        result.columns_str = columns_str;
        return result;
    }
    String path = checkpointFilePath(version);
    ReadBufferFromFile buf(path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
    while (!buf.eof())
    {
        String partName;
        Version delete_version;
        readString(partName, buf);
        assertChar('\t', buf);
        readIntText(delete_version, buf);
        assertChar('\n', buf);
        result.parts[partName] = delete_version;
    }

    // Read metadata file
    ReadBufferFromFile metadata_buf(metadataFilePath(version));
    readStringUntilEOF(result.metadata_str, metadata_buf);

    // Read columns file
    ReadBufferFromFile columns_buf(columnsFilePath(version));
    readStringUntilEOF(result.columns_str, columns_buf);

    return result;
}

void ManifestStore::discardFrom(const ManifestStore::WriteLock &, ManifestStore::Version from)
{
    checkVersionInvariant();

    auto entries = getLogEntries(from, 1);
    if (entries.empty())
        return;
    auto new_latest_version = entries.front().prev_version;
    auto new_commit_version = std::min(commitVersion(), new_latest_version);

    rocksdb::WriteBatch updates;
    updates.DeleteRange(generateLogKey(from), generateLogKey(latestVersion() + 1));
    if (new_latest_version != latestVersion())
        updates.Put(latest_version_key, toString(new_latest_version));
    if (new_commit_version != commitVersion())
        updates.Put(commit_version_key, toString(new_commit_version));

    rocksdb::WriteOptions opt;
    opt.sync = true;
    auto status = db->Write(opt, &updates);
    if (!status.ok())
        throw Exception("Failed to discard manifest logs >= " + toString(from), ErrorCodes::MANIFEST_OPERATION_ERROR);

    auto old_state = stateSummary();
    latest_version.store(new_latest_version, std::memory_order_release);
    commit_version.store(new_commit_version, std::memory_order_release);
    LOG_INFO(logger, "Discarded all manifest logs >= {} , changed from {} to {}", from, old_state, stateSummary());

    // Guarantee version invariant still holds.
    checkVersionInvariant();
}

void ManifestStore::resetToCheckpoint(Version version, const Snapshot & snapshot)
{
    checkOpened();
    checkVersionIsValid(version);
    checkVersionInvariant();

    auto lock = checkpointLock();
    bool checkpoint_changed = checkpointVersion() != version;

    if (checkpoint_changed)
        writeCheckpointFile(version, snapshot);

    rocksdb::WriteBatch updates;
    updates.DeleteRange(generateLogKey(checkpointVersion()), generateLogKey(latestVersion() + 1));
    updates.Put(checkpoint_version_key, toString(version));
    updates.Put(commit_version_key, toString(version));
    updates.Put(latest_version_key, toString(version));
    updates.Put(latest_metadata_key, snapshot.metadata_str);
    updates.Put(latest_columns_key, snapshot.columns_str);

    rocksdb::WriteOptions opt;
    opt.sync = true;
    auto status = db->Write(opt, &updates);
    if (!status.ok())
        throw Exception("Failed to reset manifest checkpoint version to " + toString(version), ErrorCodes::MANIFEST_OPERATION_ERROR);

    Version old_version = checkpointVersion();
    auto old_state = stateSummary();
    checkpoint_version.store(version, std::memory_order_release);
    commit_version.store(version, std::memory_order_release);
    latest_version.store(version, std::memory_order_release);
    metadata_str = snapshot.metadata_str;
    columns_str = snapshot.columns_str;

    /// remove old checkpoint file if exists
    if (checkpoint_changed && old_version != 0)
    {
        auto path = checkpointFilePath(old_version);
        Poco::File old_file(path);
        old_file.remove();
        Poco::File old_metadata_file(metadataFilePath(old_version));
        old_metadata_file.remove();
        Poco::File old_columns_file(columnsFilePath(old_version));
        old_columns_file.remove();
        LOG_DEBUG(logger, "Removed old checkpoint file {}", path);
    }
    LOG_INFO(
        logger,
        "Reset manifest checkpoint, changed from {} to {} with {} parts in total, with schema  {} \n {}", old_state, stateSummary(), snapshot.parts.size(), metadata_str, columns_str);
    // Guarantee version invariant still holds.
    checkVersionInvariant();
}

inline String ManifestStore::stateSummary() const
{
    std::ostringstream ss;
    ss << "{ checkpoint=" << checkpointVersion() << ", commit=" << commitVersion() << ", latest=" << latestVersion() << " }";
    return ss.str();
}

void ManifestStore::putPartRemovedVersion(const Strings & parts, ManifestStore::Version version)
{
    if (parts.empty())
        return;
    checkVersionIsValid(version);
    std::unique_lock<std::mutex> lock(mutex_removed_cache);
    for (auto & part : parts)
    {
        removed_parts_cache[part] = version;
        LOG_TRACE(logger, "Put part remove version of {} to {}", part, version);
    }
}

ManifestStore::Version ManifestStore::getPartRemovedVersion(const String & part_name) const
{
    std::unique_lock<std::mutex> lock(mutex_removed_cache);
    if (auto it = removed_parts_cache.find(part_name); it != removed_parts_cache.end())
        return it->second;
    return INVALID_VERSION;
}

void ManifestStore::deletePartRemovedVersion(const Strings & parts)
{
    if (parts.empty())
        return;
    std::unique_lock<std::mutex> lock(mutex_removed_cache);
    for (auto & part : parts)
    {
        removed_parts_cache.erase(part);
        LOG_TRACE(logger, "Delete part remove version of {}", part);
    }
}

void ManifestStore::clearAllPartRemovedVersion()
{
    std::unique_lock<std::mutex> lock(mutex_removed_cache);
    removed_parts_cache.clear();
    LOG_TRACE(logger, "Clear part removed version cache");
}

bool ManifestStore::lookForLatestTableSchema()
{
    checkOpened();

    LOG_TRACE(logger, "Loading table schema from the manifest store...");

    auto keys = {
        rocksdb::Slice(latest_metadata_key),
        rocksdb::Slice(latest_columns_key)
    };
    Strings values(2);
    auto result = db->MultiGet(rocksdb::ReadOptions(), keys, &values);
    for (auto & st : result)
    {
        if (st.ok())
        {
            // Do nothing
        }
        else if (st.IsNotFound())
        {
            return false;
        }
        else
        {
            throw Exception("Failed to read table schema from rocksdb", ErrorCodes::MANIFEST_OPERATION_ERROR);
        }
    }

    metadata_str = values[0];
    columns_str = values[1];
    if (metadata_str.empty() || columns_str.empty()) {
        throw Exception("Expected table schema not empty", ErrorCodes::MANIFEST_OPERATION_ERROR);
    }

    LOG_DEBUG(logger, "Loaded table schema from the manifest store\nmetadata:\n {} \ncolumns:\n {}", metadata_str, columns_str);
    if (checkpointVersion() > 0)
    {
        String checkpoint_path = checkpointFilePath(checkpointVersion());
        if (!Poco::File(checkpoint_path).exists())
            throw Exception("Checkpoint file must exist at version: " + toString(checkpointVersion()), ErrorCodes::MANIFEST_OPERATION_ERROR);

        auto metadata_path = metadataFilePath(checkpointVersion());
        auto columns_path = columnsFilePath(checkpointVersion());
        /// this should rarely happens, it often means bugs around the checkpoint logic.
        if (!Poco::File(metadata_path).exists() || !Poco::File(columns_path).exists())
        {
            LOG_WARNING(logger, "Checkpoint at version {} is missing schema files, recovering using current schema...", checkpoint_version);

            /// Write table schema metadata
            WriteBufferFromFile metadata_buf(metadata_path, 4096);
            writeString(metadata_str, metadata_buf);
            metadata_buf.sync();
            LOG_INFO(logger, "Wrote {} using current metadata:\n {}", metadata_path, metadata_str);

            /// Write table schema columns
            WriteBufferFromFile columns_buf(columns_path, 4096);
            writeString(columns_str, columns_buf);
            columns_buf.sync();
            LOG_INFO(logger, "Wrote {} using current columns:\n {}", metadata_path, columns_str);
        }
    }

    return true;
}
}
