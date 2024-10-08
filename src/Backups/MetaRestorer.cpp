#include <Backups/BackupUtils.h>
#include <Backups/MetaRestorer.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/formatAST.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <common/scope_guard_safe.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int RESTORE_TABLE_ALREADY_EXIST;
}

namespace MetaRestorer
{
    bool hasCompatibleDataToRestoreTable(const ASTCreateQuery & query1, const ASTCreateQuery & query2)
    {
        /// TODO: Write more subtle condition here.
        auto q1 = typeid_cast<std::shared_ptr<ASTCreateQuery>>(query1.clone());
        auto q2 = typeid_cast<std::shared_ptr<ASTCreateQuery>>(query2.clone());

        /// Remove UUIDs.
        q1->uuid = UUIDHelpers::Nil;
        q2->uuid = UUIDHelpers::Nil;

        return serializeAST(*q1) == serializeAST(*q2);
    }

    void restoreDatabase(const std::shared_ptr<ASTCreateQuery> & create_query, const ContextPtr & context)
    {
        const String & database_name = create_query->getDatabase();
        DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(database_name, context);
        if (!database)
        {
            /// We create and execute `create` query for the database name.
            // Create new transaction to create
            ContextMutablePtr create_database_context = getContextWithNewTransaction(context, false);
            SCOPE_EXIT_SAFE({
                create_database_context->getCnchTransactionCoordinator().finishTransaction(
                    create_database_context->getCurrentTransactionID());
            });
            InterpreterCreateQuery create_database_interpreter{create_query, create_database_context};
            // Transcation will be committed inside
            create_database_interpreter.execute();
        }
    }


    void createTableImpl(
        const std::shared_ptr<ASTCreateQuery> & create_query,
        std::optional<std::vector<ASTPtr>> previous_versions,
        const ContextPtr & context)
    {
        ContextMutablePtr create_table_context = getContextWithNewTransaction(context, false);
        SCOPE_EXIT_SAFE(
            { create_table_context->getCnchTransactionCoordinator().finishTransaction(create_table_context->getCurrentTransactionID()); });

        InterpreterCreateQuery create_table_interpreter{create_query, create_table_context};
        create_table_interpreter.execute();

        if (previous_versions && !previous_versions->empty())
        {
            UInt64 transaction_id = context->getCurrentTransactionID();
            for (int i = previous_versions->size() - 1; i >= 0; i--)
            {
                // hack a fake transaction
                transaction_id -= 1;
                Protos::DataModelTable table;
                table.set_database(create_query->getDatabase());
                table.set_name(create_query->getTable());
                RPCHelpers::fillUUID(create_query->uuid, *(table.mutable_uuid()));
                table.set_definition(serializeAST(*previous_versions.value()[i]));
                table.set_txnid(transaction_id);
                table.set_commit_time(transaction_id);
                // if (i > 0)
                //     table.set_previous_version(transaction_id - 1);
                context->getCnchCatalog()->restoreTableHistoryVersion(table);
            }
        }
    }

    void restoreTable(
        const std::shared_ptr<ASTCreateQuery> & create_query,
        std::optional<std::vector<ASTPtr>> previous_versions,
        const ContextPtr & context)
    {
        DatabaseAndTableName table_name{create_query->getDatabase(), create_query->getTable()};
        const String & database_name = table_name.first;
        DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(database_name, context);
        // If database is not exist, create both database and table and return.
        if (!database)
        {
            /// We create and execute `create` query for the database name.
            auto create_database_query = std::make_shared<ASTCreateQuery>();
            create_database_query->setDatabase(database_name);
            create_database_query->if_not_exists = true;
            // Create new transaction to create
            ContextMutablePtr create_database_context = getContextWithNewTransaction(context, false);
            SCOPE_EXIT_SAFE({
                create_database_context->getCnchTransactionCoordinator().finishTransaction(
                    create_database_context->getCurrentTransactionID());
            });
            InterpreterCreateQuery create_database_interpreter{create_database_query, create_database_context};
            // Transcation will be committed inside
            create_database_interpreter.execute();

            // If database does not exist, neither does table
            createTableImpl(create_query, previous_versions, context);
            return;
        }

        // If restore table is already exist, check its metadata same with that of backup table
        StoragePtr existing_storage = database->tryGetTable(table_name.second, context);
        if (existing_storage)
        {
            // If table in backup has multiple version, we don't allow it to restore to existing storage
            if (previous_versions && !previous_versions->empty())
                throw Exception(
                    ErrorCodes::RESTORE_TABLE_ALREADY_EXIST,
                    "Table {} in backup has multiple version, it's only allowed to restore to non-existing table.",
                    table_name.second);

            // Only allow to restore to empty table
            if (!context->getCnchCatalog()
                     ->getAllServerDataParts(existing_storage, context->getCurrentTransactionID(), context.get())
                     .empty())
                throw Exception(
                    ErrorCodes::RESTORE_TABLE_ALREADY_EXIST,
                    "Table {} exists and is not empty, it's only allowed to restore to empty table.", table_name.second);

            // Check the schema
            TableExclusiveLockHolder table_lock_holder
                = existing_storage->lockExclusively(context->getCurrentQueryId(), context->getSettingsRef().lock_acquire_timeout);
            // Check schema of existing table is the same as backup table
            auto existing_table_create_query = database->tryGetCreateTableQuery(table_name.second, context);
            const ASTCreateQuery & backup_create_query = create_query->as<ASTCreateQuery &>();
            if (!hasCompatibleDataToRestoreTable(backup_create_query, existing_table_create_query->as<ASTCreateQuery &>()))
                throw Exception(
                    ErrorCodes::RESTORE_TABLE_ALREADY_EXIST,
                    "Table " + backQuoteIfNeed(table_name.first) + "." + backQuoteIfNeed(table_name.second) + " already exists");
        }
        else
        {
            // If table is not exist, create it
            createTableImpl(create_query, previous_versions, context);
        }
    }
}
}
