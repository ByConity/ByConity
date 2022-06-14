#include <Catalog/CatalogFactory.h>
#include <Dictionaries/DictionaryFactory.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <MergeTreeCommon/CnchTableHelper.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/Status.h>
#include <Interpreters/InterpreterCreateQuery.h>

#include <Databases/DatabaseCnch.h>
#include <Protos/RPCHelpers.h>
#include <Transaction/TxnTimestamp.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CATALOG_SERVICE_INTERNAL_ERROR;
}

namespace Catalog {

CatalogFactory::DatabasePtr CatalogFactory::getDatabaseByDataModel(const DB::Protos::DataModelDB &db_model, const ContextPtr & context) {
    DatabasePtr db {nullptr};
    if (db_model.has_uuid())
    {
        db = std::make_shared<DatabaseCnch>(db_model.name(), RPCHelpers::createUUID(db_model.uuid()), context);
    }
    else
    {
        throw Exception("DataModelDB has no uuid", ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);
        //db = std::make_shared<DatabaseCnch>(db_model.name(), context);
    }
    return db;
}

StoragePtr CatalogFactory::getTableByDataModel(
        Context &context,
        const DB::Protos::DataModelTable *table_model)
{
    auto & db = table_model->database();
    auto & table = table_model->name();
    auto & create_query = table_model->definition();
    auto storagePtr = getTableByDefinition(context, db, table, create_query);
    storagePtr->commit_time =  TxnTimestamp{table_model->commit_time()};
    if (dynamic_cast<MergeTreeMetaBase*>(storagePtr.get()))
    {
        storagePtr->part_columns = std::make_shared<NamesAndTypesList>(storagePtr->getInMemoryMetadataPtr()->getColumns().getAllPhysical());
        for (auto & version : table_model->definitions())
        {
            auto s = getTableByDefinition(context, db, table, version.definition());
            storagePtr->previous_versions_part_columns[version.commit_time()] = std::make_shared<NamesAndTypesList>(s->getInMemoryMetadataPtr()->getColumns().getAllPhysical());
        }
    }
    storagePtr->is_dropped = DB::Status::isDeleted(table_model->status());
    storagePtr->is_detached = DB::Status::isDetached(table_model->status());
    return storagePtr;
}

StoragePtr CatalogFactory::getTableByDefinition(
        DB::Context &context,
        [[maybe_unused]] const DB::String &db,
        [[maybe_unused]] const DB::String &table,
        const DB::String &create) {

    auto res = CnchTableHelper::createStorageFromQuery(create, context);

    res->setCreateTableSql(create);

    return res;
}

ASTPtr CatalogFactory::getCreateDictionaryByDataModel(const DB::Protos::DataModelDictionary * dict_model)
{
    const auto & create_query = dict_model->definition();
    const char *begin = create_query.data();
    const char *end = begin + create_query.size();
    ParserQuery parser(end);
    ASTPtr ast = parseQuery(parser, begin, end, "", 0, 0);
    ASTCreateQuery *create_ast = ast->as<ASTCreateQuery>();
    if (!create_ast)
        throw Exception("Wrong dictionary definition.", ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);

    return ast;
}

}


}
