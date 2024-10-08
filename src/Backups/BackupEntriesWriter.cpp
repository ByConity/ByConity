#include <filesystem>
#include <memory>
#include <unordered_map>
#include <Backups/BackupCopyTool.h>
#include <Backups/BackupDiskInfo.h>
#include <Backups/BackupEntriesWriter.h>
#include <Backups/BackupEntryFromFile.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupUtils.h>
#include <Disks/DiskByteS3.h>
#include <Disks/IDisk.h>
#include <FormaterTool/ZipHelper.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Protos/cnch_worker_rpc.pb.h>
#include <rocksdb/db.h>
#include <Poco/Zip/Compress.h>
#include <Poco/Zip/Decompress.h>
#include <common/scope_guard_safe.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BACKUP_STATUS_ERROR;
    extern const int BACKUP_UPLOAD_INDEX_ERROR;
    extern const int UNKNOWN_BACKUP_ENTRY_TYPE;
}

BackupEntriesWriter::BackupEntriesWriter(const ASTBackupQuery & backup_query_, BackupTaskPtr & backup_task_, const ContextPtr & context_)
    : backup_query(backup_query_), backup_info(BackupDiskInfo::fromAST(*backup_query_.backup_disk)), backup_task(backup_task_), context(context_)
{
}

void BackupEntriesWriter::write(BackupEntries & backup_entries, bool need_upload_entry_file) const
{
    DiskPtr backup_disk = context->getDisk(backup_info.disk_name);

    // upload backup entries as a zip file to help recover
    if (need_upload_entry_file)
    {
        checkBackupTaskNotAborted(backup_task->getId(), context);
        backup_task->updateProgress("Upload backup entries", context);
        uploadBackupEntryFile(backup_disk, backup_info.backup_dir, backup_entries);
    }

    checkBackupTaskNotAborted(backup_task->getId(), context);
    backup_task->updateProgress("Start backing up metadata", context);

    BackupCopyTasks backup_copy_tasks;
    for (auto & backup_entry : backup_entries)
    {
        // For example, "data/table1/part1"
        String & backup_relative_path = backup_entry.first;
        // Add backup base dir, "backup_dir/data/table1/part1"
        String backup_fullpath = backup_info.backup_dir + "/" + backup_relative_path;
        if (backup_entry.second->getEntryType() == IBackupEntry::BackupEntryType::DATA)
        {
            auto * data_entry = dynamic_cast<BackupEntryFromFile *>(backup_entry.second.get());
            DiskPtr source_disk = data_entry->getDisk();
            String source_path = data_entry->getFilePath();
            backup_copy_tasks.emplace_back(
                createBackupCopyTask(source_disk->getName(), source_path, backup_disk->getName(), backup_fullpath));
        }
        else if (backup_entry.second->getEntryType() == IBackupEntry::BackupEntryType::METADATA)
        {
            std::unique_ptr<WriteBufferFromFileBase> backup_buffer = backup_disk->writeFile(backup_fullpath);
            auto source_buffer = backup_entry.second->getReadBuffer();
            copyData(*source_buffer, *backup_buffer);
            backup_buffer->finalize();
        }
    }

    sendCopyTasksToWorker(backup_task, backup_copy_tasks, context);
}

void BackupEntriesWriter::addBackupEntriesToRocksDB(
    const String & entry_dir, const BackupEntries & backup_entries, size_t & total_bytes) const
{
    rocksdb::DB * backup_entry_db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.info_log_level = rocksdb::InfoLogLevel::WARN_LEVEL;
    rocksdb::Status status = rocksdb::DB::Open(options, entry_dir, &backup_entry_db);
    if (status != rocksdb::Status::OK())
        throw Exception("Fail to open rocksdb path at: " + entry_dir + ": " + status.ToString(), ErrorCodes::BACKUP_UPLOAD_INDEX_ERROR);
    SCOPE_EXIT_SAFE({
        backup_entry_db->Close();
        delete backup_entry_db;
    });

    // A backup data entry contains backup_path, remote_disk_name, source_path, source_size
    // Assume one backup entry is about 200 Byte, we keep 100K entries as a batch, which is up to 20M.
    size_t batch_size = 100000;
    rocksdb::WriteBatch batch;
    for (size_t i = 0; i < backup_entries.size(); i++)
    {
        const auto & backup_entry = backup_entries[i].second;
        Protos::DataModelBackupEntry entry_model;
        entry_model.set_entry_type(static_cast<UInt32>(backup_entry->getEntryType()));
        entry_model.set_file_size(backup_entry->getSize());
        total_bytes += backup_entry->getSize();
        if (backup_entry->getEntryType() == IBackupEntry::BackupEntryType::METADATA)
        {
            auto * meta_entry = dynamic_cast<BackupEntryFromMemory *>(backup_entry.get());
            entry_model.set_inline_value(meta_entry->getData());
        }
        else
        {
            auto * data_entry = dynamic_cast<BackupEntryFromFile *>(backup_entry.get());
            entry_model.set_source_disk(data_entry->getDisk()->getName());
            entry_model.set_source_path(data_entry->getFilePath());
        }

        status = batch.Put(backup_entries[i].first, entry_model.SerializeAsString());
        if (!status.ok())
            throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::BACKUP_UPLOAD_INDEX_ERROR);

        if ((i + 1) % batch_size == 0 || i == backup_entries.size() - 1)
        {
            status = backup_entry_db->Write(rocksdb::WriteOptions(), &batch);
            if (!status.ok())
                throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::BACKUP_UPLOAD_INDEX_ERROR);
            batch.Clear();
        }
    }

    // the lifecycle of rocksdb is over here
    backup_entry_db->Flush(rocksdb::FlushOptions());
}


void BackupEntriesWriter::loadBackupEntriesFromRocksDB(
    const String & entry_dir, const DiskPtr & backup_disk, BackupEntries & backup_entries) const
{
    // initialize rocksdb in entry_dir
    rocksdb::DB * backup_entry_db;
    rocksdb::Options options;
    options.create_if_missing = false;
    options.info_log_level = rocksdb::InfoLogLevel::WARN_LEVEL;
    rocksdb::Status status = rocksdb::DB::Open(options, entry_dir, &backup_entry_db);
    if (status != rocksdb::Status::OK())
        throw Exception("Fail to open rocksdb path at: " + entry_dir + ": " + status.ToString(), ErrorCodes::BACKUP_UPLOAD_INDEX_ERROR);

    // 2. check already finished backup files, update backup entry list
    rocksdb::Iterator * iterator = backup_entry_db->NewIterator(rocksdb::ReadOptions());
    SCOPE_EXIT_SAFE({
        delete iterator;
        backup_entry_db->Close();
        delete backup_entry_db;
    });

    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next())
    {
        String file_relative_path = iterator->key().ToString();
        Protos::DataModelBackupEntry entry_model;
        entry_model.ParseFromString(iterator->value().ToString());

        size_t file_size = entry_model.file_size();
        // Add backup base dir, "backup_dir/data/table1/part1"
        String file_fullpath = backup_info.backup_dir + "/" + file_relative_path;
        // TODO: Use progress to filter entries
        bool is_file_exist = backup_disk->fileExists(file_fullpath);
        if (is_file_exist && backup_disk->getFileSize(file_fullpath) == file_size)
            continue;
        else
        {
            // Remove broken file
            if (is_file_exist)
                backup_disk->removeFile(file_fullpath);

            std::unique_ptr<IBackupEntry> backup_entry;
            std::unordered_map<String, DiskPtr> disk_map;
            if (entry_model.entry_type() == 0)
            {
                backup_entries.emplace_back(file_relative_path, std::make_unique<BackupEntryFromMemory>(entry_model.inline_value()));
            }
            else if (entry_model.entry_type() == 1)
            {
                DiskPtr source_disk;
                // Avoid get disk from context frequently, which need a lock
                if (disk_map.find(entry_model.source_disk()) != disk_map.end())
                    source_disk = disk_map[entry_model.source_disk()];
                else
                {
                    source_disk = context->getDisk(entry_model.source_disk());
                    disk_map[entry_model.source_disk()] = source_disk;
                }

                backup_entries.emplace_back(file_relative_path, std::make_unique<BackupEntryFromFile>(source_disk, entry_model.source_path()));
            }
            else
                throw Exception(ErrorCodes::UNKNOWN_BACKUP_ENTRY_TYPE, "Unknown backup entry type {}", entry_model.entry_type());
        }
    }
}

// Serialize backup entries to rocksDB, and upload it
void BackupEntriesWriter::uploadBackupEntryFile(
    DiskPtr & backup_disk, const String & backup_dir, const BackupEntries & backup_entries) const
{
    DiskPtr temp_disk = context->getTemporaryVolume()->getDisk();
    String backup_temp_dir = fs::path(temp_disk->getPath()) / "backup_tmp" / backup_task->getId();
    if (temp_disk->exists(backup_temp_dir))
    {
        LOG_WARNING(getLogger("BackupEntriesCollector"), "Backup temp directory for RocksDB is already exist, delete it.");
        temp_disk->removeRecursive(backup_temp_dir);
    }
    String entry_dir = backup_temp_dir + "/entries/";
    context->getTemporaryVolume()->getDisk()->createDirectories(entry_dir);

    SCOPE_EXIT_SAFE(fs::remove_all(backup_temp_dir)); // clean up temp dir

    size_t total_bytes = 0;
    addBackupEntriesToRocksDB(entry_dir, backup_entries, total_bytes);

    // pack rocksdb as a zip file
    String zip_path = fs::path(backup_temp_dir) / getBackupEntryFilePathInBackup();
    std::ofstream out(zip_path, std::ios::binary);
    Poco::Zip::Compress compress(out, true);
    compress.addRecursive(entry_dir, Poco::Zip::ZipCommon::CompressionMethod::CM_STORE, Poco::Zip::ZipCommon::CL_SUPERFAST);
    compress.close();
    out.close();

    // upload rocksdb zip file to backup disk in remote
    size_t zip_file_size = std::filesystem::file_size(zip_path);
    // If server restarts while zip file is uploading, this zip file may be truncated and broken.
    // To recognize this, we have to use a meta file by comparing the filesize.
    String backup_entry_meta_path = backup_dir + "/" + getBackupEntryMetaFilePathInBackup();
    auto meta_write_buffer = backup_disk->writeFile(backup_entry_meta_path);
    writeIntBinary(zip_file_size, *meta_write_buffer);
    meta_write_buffer->finalize();

    String backup_entry_file_path = backup_dir + "/" + getBackupEntryFilePathInBackup();
    auto write_buffer = backup_disk->writeFile(backup_entry_file_path);
    ReadBufferFromFile from_buffer(zip_path);
    copyData(from_buffer, *write_buffer);
    write_buffer->finalize();

    total_bytes += zip_file_size + sizeof(zip_file_size);
    backup_task->updateTotalBackupBytes(total_bytes, context);
}

void BackupEntriesWriter::write(String & backup_entry_remote_path) const
{
    // 1. get backup entries from remote, do filtering
    try
    {
        DiskPtr backup_disk = context->getDisk(backup_info.disk_name);
        BackupEntries backup_entries;
        getBackupEntriesFromRemote(backup_disk, backup_entry_remote_path, backup_entries);

        // 2. write backup entries
        write(backup_entries, false);
    }
    catch (...)
    {
        throw;
    }
}

void BackupEntriesWriter::getBackupEntriesFromRemote(
    DiskPtr & backup_disk, String & backup_entry_remote_path, BackupEntries & backup_entries) const
{
    checkBackupTaskNotAborted(backup_task->getId(), context);
    backup_task->updateProgress("Download backup entries", context);

    // 1. download backup entry zip file from remote
    DiskPtr temp_disk = context->getTemporaryVolume()->getDisk();
    String backup_temp_dir = fs::path(temp_disk->getPath()) / "backup_tmp" / backup_task->getId();
    if (temp_disk->exists(backup_temp_dir))
    {
        LOG_WARNING(getLogger("BackupEntriesCollector"), "Backup temp directory for RocksDB is already exist, delete it.");
        temp_disk->removeRecursive(backup_temp_dir);
    }
    String entry_dir = backup_temp_dir + "/entries/";
    temp_disk->createDirectories(entry_dir);
    // delete temp files
    SCOPE_EXIT_SAFE(fs::remove_all(backup_temp_dir));

    String local_zip_path = fs::path(backup_temp_dir) / getBackupEntryFilePathInBackup();
    WriteBufferFromFile local_buffer(local_zip_path);
    auto remote_buffer = backup_disk->readFile(backup_entry_remote_path);
    copyData(*remote_buffer, local_buffer);
    local_buffer.finalize();

    // decompress entry zip file to backup entry directory
    std::ifstream in(local_zip_path, std::ios::binary);
    Poco::Zip::Decompress decompress(in, entry_dir, true);
    decompress.decompressAllFiles();

    loadBackupEntriesFromRocksDB(entry_dir, backup_disk, backup_entries);
}
}
