#include <Backups/BackupUtils.h>
#include <Catalog/Catalog.h>
#include <Core/UUID.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Common/escapeForFileName.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BACKUP_STATUS_ERROR;
}

void checkBackupTaskNotAborted(const String & backup_id, const ContextPtr & context)
{
    BackupTaskModel backup_task = context->getCnchCatalog()->tryGetBackupJob(backup_id);
    if (backup_task
        && (getBackupStatus(backup_task->status()) == BackupStatus::ABORTED
            || getBackupStatus(backup_task->status()) == BackupStatus::FAILED))
        throw Exception(ErrorCodes::BACKUP_STATUS_ERROR, "Backup job aborted or failed.");
}

String getDataPathInBackup(const String & table_uuid)
{
    if (table_uuid.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table uuid must not be empty");
    return String{"data/"} + table_uuid + "/";
}

String getDataPathInBackup(const IAST & create_query)
{
    const auto & create = create_query.as<const ASTCreateQuery &>();
    if (create.table.empty())
        return {};
    return getDataPathInBackup(UUIDHelpers::UUIDToString(create.uuid));
}

String getDeleteFilesPathInBackup(String data_path_in_backup)
{
    return data_path_in_backup + DeleteBitmapMeta::delete_files_dir + "/";
}

String getDeleteFilesPathInBackup(const IAST & create_query)
{
    return getDataPathInBackup(create_query) + DeleteBitmapMeta::delete_files_dir + "/";
}

String getPartFilesPathInBackup(String data_path_in_backup)
{
    return data_path_in_backup + "parts/";
}

String getPartFilesPathInBackup(const IAST & create_query)
{
    return getDataPathInBackup(create_query) + "parts/";
}

String getMetadataPathInBackup(const DatabaseAndTableName & table_name)
{
    if (table_name.first.empty() || table_name.second.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name and table name must not be empty");
    return String{"metadata/"} + escapeForFileName(table_name.first) + "/" + escapeForFileName(table_name.second) + ".sql";
}

String getMetaHistoryPathInBackup(const DatabaseAndTableName & table_name)
{
    if (table_name.first.empty() || table_name.second.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name and table name must not be empty");
    return String{"metadata/"} + escapeForFileName(table_name.first) + "/" + escapeForFileName(table_name.second) + ".history";
}

String getMetaHistoryPathInBackup(const IAST & create_query)
{
    const auto & create = create_query.as<const ASTCreateQuery &>();
    return getMetaHistoryPathInBackup({create.getDatabase(), create.getTable()});
}

String getMetadataPathInBackup(const String & database_name)
{
    if (database_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name must not be empty");
    return String{"metadata/"} + escapeForFileName(database_name) + ".sql";
}

String getMetadataPathInBackup(const IAST & create_query)
{
    const auto & create = create_query.as<const ASTCreateQuery &>();
    if (create.table.empty())
        return getMetadataPathInBackup(create.getDatabase());
    return getMetadataPathInBackup({create.getDatabase(), create.getTable()});
}

String getBackupEntryMetaFilePathInBackup()
{
    return "entries.meta";
}

String getBackupEntryFilePathInBackup()
{
    return "entries.zip";
}

ASTPtr readCreateQueryFromBackup(const String & database_name, const DiskPtr & backup_disk, const String & base_backup_path)
{
    String create_query_path = getMetadataPathInBackup(database_name);
    auto read_buffer = backup_disk->readFile(fs::path(base_backup_path) / create_query_path);
    String create_query_str;
    readStringUntilEOF(create_query_str, *read_buffer);
    read_buffer.reset();
    ParserCreateQuery create_parser;
    return parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
}

ASTPtr readCreateQueryFromBackup(const DatabaseAndTableName & table_name, const DiskPtr & backup_disk, const String & base_backup_path)
{
    String create_query_path = getMetadataPathInBackup(table_name);
    auto read_buffer = backup_disk->readFile(fs::path(base_backup_path) / create_query_path);
    String create_query_str;
    readStringUntilEOF(create_query_str, *read_buffer);
    read_buffer.reset();
    ParserCreateQuery create_parser;
    return parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
}

std::optional<std::vector<ASTPtr>>
readTableHistoryFromBackup(const DatabaseAndTableName & table_name, const DiskPtr & backup_disk, const String & base_backup_path)
{
    String meta_history_path = getMetaHistoryPathInBackup(table_name);
    if (!backup_disk->exists(fs::path(base_backup_path) / meta_history_path))
        return std::nullopt;

    auto read_buffer = backup_disk->readFile(fs::path(base_backup_path) / meta_history_path);
    // For future expand
    UInt64 backup_version_number;
    readIntBinary(backup_version_number, *read_buffer);
    size_t previous_version_size = 0;
    readIntBinary(previous_version_size, *read_buffer);

    std::vector<ASTPtr> previous_versions;
    for (size_t i = 0; i < previous_version_size; i++)
    {
        String version;
        readStringBinary(version, *read_buffer);
        ParserCreateQuery create_parser;
        previous_versions.emplace_back(parseQuery(create_parser, version, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH));
    }
    return previous_versions;
}

// Return a context copy with new transcation
ContextMutablePtr getContextWithNewTransaction(const ContextPtr & context, bool read_only)
{
    ContextMutablePtr new_context;
    if (context->hasSessionContext())
    {
        new_context = Context::createCopy(context->getSessionContext());
    }
    else
    {
        new_context = Context::createCopy(context->getGlobalContext());
    }

    new_context->setSettings(context->getSettings());

    if (context->tryGetCurrentWorkerGroup())
    {
        new_context->setCurrentVW(context->getCurrentVW());
        new_context->setCurrentWorkerGroup(context->getCurrentWorkerGroup());
    }

    auto txn = new_context->getCnchTransactionCoordinator().createTransaction(
        CreateTransactionOption()
            .setContext(new_context)
            .setForceCleanByDM(context->getSettingsRef().force_clean_transaction_by_dm)
            .setAsyncPostCommit(context->getSettingsRef().async_post_commit)
            .setReadOnly(read_only));
    if (txn)
    {
        new_context->setCurrentTransaction(txn);
    }
    else
    {
        throw Exception("Failed to create transaction", ErrorCodes::LOGICAL_ERROR);
    }
    return new_context;
}
}
