#include <filesystem>
#include <optional>
#include <Access/AccessType.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromFile.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupRenamingConfig.h>
#include <Backups/BackupStatus.h>
#include <Backups/BackupUtils.h>
#include <Backups/IBackupEntry.h>
#include <Backups/renameInCreateQuery.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <CloudServices/CnchPartsHelper.h>
#include <Core/UUID.h>
#include <Disks/DiskHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/data_models.pb.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Common/Coding.h>
#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BACKUP_JOB_UNSUPPORTED;
    extern const int BACKUP_STATUS_ERROR;
    extern const int BACKUP_UPLOAD_INDEX_ERROR;
    extern const int BACKUP_ENTRIES_EXCEED_MAX;
}

BackupEntriesCollector::BackupEntriesCollector(
    const ASTBackupQuery & backup_query_,
    BackupTaskPtr & backup_task_,
    const ContextPtr & context_,
    size_t max_backup_entries_,
    UInt64 snapshot_ts_)
    : backup_query(backup_query_)
    , backup_task(backup_task_)
    , context(context_)
    , max_backup_entries(max_backup_entries_)
    , snapshot_ts(snapshot_ts_)
{
}

static void backupCreateQuery(const IAST & create_query, std::optional<Strings> previous_versions, BackupEntries & backup_entries)
{
    auto metadata_entry = std::make_unique<BackupEntryFromMemory>(serializeAST(create_query));
    String metadata_path = getMetadataPathInBackup(create_query);
    backup_entries.emplace_back(metadata_path, std::move(metadata_entry));

    // handle previous version if exist
    if (previous_versions && !previous_versions->empty())
    {
        String metadata_history_path = getMetaHistoryPathInBackup(create_query);
        WriteBufferFromOwnString write_buffer;
        // First, write version number
        UInt64 backup_schema_version = BACKUP_SCHEMA_VERSION;
        writeIntBinary(backup_schema_version, write_buffer);
        size_t previous_version_size = previous_versions->size();
        writeIntBinary(previous_version_size, write_buffer);
        for (const auto & previous_version : *previous_versions)
        {
            ParserCreateQuery create_parser;
            ASTPtr history_table = parseQuery(create_parser, previous_version, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
            // To serialize as one line
            writeStringBinary(serializeAST(*history_table), write_buffer);
        }
        auto metadata_history_entry = std::make_unique<BackupEntryFromMemory>(write_buffer.str());
        backup_entries.emplace_back(metadata_history_path, std::move(metadata_history_entry));
    }
}

void BackupEntriesCollector::collectTableEntries(
    const DatabaseAndTableName & database_table, const BackupRenamingConfigPtr & renaming_config, std::optional<ASTs> partitions)
{
    // It's a optimistic judgement. It means we can accept exceeding the maximum limit slightly after collecting the following one table.
    if (backup_entries.size() >= max_backup_entries)
        throw Exception(
            ErrorCodes::BACKUP_ENTRIES_EXCEED_MAX,
            "The number of backup entries exceeded the maximum limit {}, current size is {}. You can modify the max_backup_entries "
            "parameter in the backup settings.",
            max_backup_entries,
            backup_entries.size());
    checkBackupTaskNotAborted(backup_task->getId(), context);
    const String & database_name = database_table.first;
    const String & table_name = database_table.second;
    backup_task->updateProgress("Collecting backup entries for table " + database_name + "." + table_name, context);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(database_name, context);
    StoragePtr table_storage = database->tryGetTable(table_name, context);

    try
    {
        // Collect table related DDL
        auto create_table_query = database->getCreateTableQuery(table_name, context);
        ASTPtr new_create_query = renameInCreateQuery(create_table_query, renaming_config, context);
        backupCreateQuery(
            *new_create_query,
            context->getCnchCatalog()->getTableAllPreviousDefinitions(UUIDHelpers::UUIDToString(table_storage->getStorageUUID())),
            backup_entries);

        // Collect table data entry
        std::shared_ptr<StorageCnchMergeTree> cnch_merge_tree = dynamic_pointer_cast<StorageCnchMergeTree>(table_storage);
        if (!cnch_merge_tree)
            return;

        std::optional<Strings> partition_ids;
        if (partitions)
        {
            partition_ids = std::make_optional<Strings>();
            for (const ASTPtr & partition : *partitions)
                partition_ids->emplace_back(cnch_merge_tree->getPartitionIDFromQuery(partition, context));
        }
        ServerDataPartsWithDBM data_parts = cnch_merge_tree->getServerDataPartsWithDBMFromSnapshot(context, partition_ids, snapshot_ts);
        if (data_parts.first.empty())
        {
            LOG_TRACE(getLogger("BackupEntriesCollector"), "No part can be backup for table {}", database_table.second);
            return;
        }
        // Construct part chain, so that we can get only one delete bitmap for one part chain
        auto visible_parts = CnchPartsHelper::calcVisibleParts(data_parts.first, false);
        if (cnch_merge_tree->getInMemoryMetadataPtr()->hasUniqueKey())
            cnch_merge_tree->getDeleteBitmapMetaForServerParts(visible_parts, data_parts.second);

        UUID table_uuid = table_storage->getStorageUUID();
        if (!visible_parts.empty())
        {
            String part_backup_dir = getPartFilesPathInBackup(*new_create_query);
            String delete_files_backup_dir = getDeleteFilesPathInBackup(*new_create_query);
            for (const ServerDataPartPtr & part : visible_parts)
            {
                DataPartInfoPtr part_info;
                for (ServerDataPartPtr current_part = part; current_part != nullptr; current_part = current_part->tryGetPreviousPart())
                {
                    const Protos::DataModelPart & part_model = current_part->part_model();
                    part_info = createPartInfoFromModel(part_model.part_info());
                    UInt32 path_id = part_model.has_data_path_id() ? part_model.data_path_id() : 0;
                    DiskPtr remote_disk = getDiskForPathId(table_storage->getStoragePolicy(IStorage::StorageLocation::MAIN), path_id);

                    // Calculate relative path to disk
                    String relative_path;
                    switch (remote_disk->getType())
                    {
                        case DiskType::Type::ByteS3: {
                            UUID part_id = RPCHelpers::createUUID(part_model.part_id());
                            relative_path = UUIDHelpers::UUIDToString(part_id);
                            break;
                        }
                        case DiskType::Type::ByteHDFS: {
                            relative_path = UUIDHelpers::UUIDToString(table_uuid) + "/" + part_info->getPartNameWithHintMutation();
                            break;
                        }
                        default:
                            throw Exception(
                                ErrorCodes::BACKUP_JOB_UNSUPPORTED, "Unsupported disk type {}", DiskType::toString(remote_disk->getType()));
                    }

                    String part_backup_path = part_backup_dir + part_info->getPartNameWithHintMutation() + "/data";
                    backup_entries.emplace_back(
                        part_backup_path, std::make_unique<BackupEntryFromFile>(remote_disk, relative_path + "/data"));
                }

                // Handle delete files
                if (cnch_merge_tree->getInMemoryMetadataPtr()->hasUniqueKey())
                {
                    const ImmutableDeleteBitmapPtr & bitmap = part->getDeleteBitmap(*cnch_merge_tree, false);

                    if (bitmap)
                    {
                        WriteBufferFromOwnString meta_string;
                        writeIntBinary(DeleteBitmapMeta::delete_file_meta_format_version, meta_string);
                        writeIntBinary(bitmap->cardinality(), meta_string);

                        if (bitmap->cardinality() <= DeleteBitmapMeta::kInlineBitmapMaxCardinality)
                        {
                            // If inline-value, store content in .meta file directly
                            String value;
                            value.reserve(bitmap->cardinality() * sizeof(UInt32));
                            for (auto it = bitmap->begin(); it != bitmap->end(); ++it)
                                PutFixed32(&value, *it);
                            writeStringBinary(value, meta_string);
                        }
                        else
                        {
                            // Else, store file size in .meta file and file content in .bitmap file
                            size_t size = bitmap->getSizeInBytes();
                            PODArray<char> bitmap_buf(size);
                            size = bitmap->write(bitmap_buf.data());
                            writeIntBinary(size, meta_string);
                            // backup_path/DeleteFiles/part_name.bitmap
                            String delete_file_backup_path = delete_files_backup_dir + part_info->getPartNameWithHintMutation() + ".bitmap";
                            backup_entries.emplace_back(
                                delete_file_backup_path, std::make_unique<BackupEntryFromMemory>(bitmap_buf.data(), size));
                        }
                        // backup_path/DeleteFiles/part_name.meta
                        String delete_meta_backup_path = delete_files_backup_dir + part_info->getPartNameWithHintMutation() + ".meta";
                        backup_entries.emplace_back(delete_meta_backup_path, std::make_unique<BackupEntryFromMemory>(meta_string.str()));
                    }
                    else
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bitmap of unique table part {} is null", part->get_name());
                }
            }
        }
    }
    catch (Exception & e)
    {
        e.addMessage("Error while collecting data of {}.{} for backup", database_name, table_name);
        throw;
    }
}

void BackupEntriesCollector::collectDatabaseEntries(
    const String & database_name, std::set<DatabaseAndTableName> except_tables, const BackupRenamingConfigPtr & renaming_config)
{
    checkBackupTaskNotAborted(backup_task->getId(), context);
    backup_task->updateProgress("Collecting backup entries for database " + database_name, context);

    // Collect database related DDL
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(database_name, context);
    auto create_database_query = database->getCreateDatabaseQuery();
    ASTPtr new_create_query = renameInCreateQuery(create_database_query, renaming_config, context);
    backupCreateQuery(*new_create_query, std::nullopt, backup_entries);

    // Collect table related DDL
    std::unordered_set<String> except_table_names;
    if (!except_tables.empty())
    {
        for (const auto & except_table : except_tables)
        {
            if (except_table.first == database->getDatabaseName())
                except_table_names.emplace(except_table.second);
        }
    }
    auto filter_by_table_name = [&except_table_names](const String & table_name) { return !except_table_names.contains(table_name); };
    for (auto it = database->getTablesIterator(context, filter_by_table_name); it->isValid(); it->next())
    {
        StoragePtr table = it->table();
        if (table == nullptr)
            continue; /// Probably the table has been just dropped.
        collectTableEntries({database_name, table->getTableName()}, renaming_config, std::nullopt);
    }
}

BackupEntries & BackupEntriesCollector::collect()
{
    checkBackupTaskNotAborted(backup_task->getId(), context);

    // Parse renaming config
    auto renaming_config = std::make_shared<BackupRenamingConfig>();
    renaming_config->setFromBackupQueryElements(backup_query.elements);

    // Collect backup entries
    for (const auto & element : backup_query.elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE: {
                collectTableEntries({element.database_name, element.table_name}, renaming_config, element.partitions);
                break;
            }

            case ASTBackupQuery::DATABASE: {
                collectDatabaseEntries(element.database_name, element.except_tables, renaming_config);
                break;
            }

            default:
                throw Exception(
                    "Unexpected element type",
                    ErrorCodes::BACKUP_JOB_UNSUPPORTED); /// other element types have been removed in deduplicateElements()
        }
    }
    return backup_entries;
}
}
