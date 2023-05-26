#include "HiveExternalCatalog.h"
#include <vector>
#include <hive_metastore_types.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <common/logger_useful.h>
#include "IExternalCatalogMgr.h"
#include "Interpreters/Context_fwd.h"
#include "Metastore/HiveMetastore.h"
#include "Parsers/formatAST.h"
#include "Storages/Hive/HiveSchemaConverter.h"
namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::ExternalCatalog
{

static std::string key_hive_uri = "hive.metastore.uri";
HiveExternalCatalog::HiveExternalCatalog(const std::string & _catalog_name, [[maybe_unused]] PlainConfigsPtr conf)
    : catalog_name(_catalog_name), configs(new PlainConfigs())
{
    conf->copyTo(*this->configs);
    if (!configs->has(key_hive_uri))
    {
        throw Exception(fmt::format("{} is not configured for {}", key_hive_uri, catalog_name), ErrorCodes::BAD_ARGUMENTS);
    }
    configs->forEachKey([this](const std::string & key, const std::string & value) { LOG_DEBUG(log, "{} - {}", key, value); });
    std::string hive_uri = configs->getString(key_hive_uri);
    LOG_DEBUG(log, "hive_uri {}", hive_uri);
    hms_client = HiveMetastoreClientFactory::instance().getOrCreate(hive_uri);
}

std::vector<std::string> HiveExternalCatalog::listDbNames()
{
    return hms_client->getAllDatabases();
}
std::vector<std::string> HiveExternalCatalog::listTableNames(const std::string & db_name)
{
    return hms_client->getAllTables(db_name);
}
std::vector<std::string>
HiveExternalCatalog::listPartitionNames([[maybe_unused]] const std::string & db_name, [[maybe_unused]] const std::string & table_name)
{
    return {};
}
std::vector<ApacheHive::Partition>
HiveExternalCatalog::getPartionsByFilter(const std::string & db_name, const std::string & table_name, const std::string & filter)
{
    return hms_client->getPartitionsByFilter(db_name, table_name, filter);
}


DB::StoragePtr HiveExternalCatalog::getTable(
    [[maybe_unused]] const std::string & db_name,
    [[maybe_unused]] const std::string & table_name,
    [[maybe_unused]] ContextPtr local_context)
{
    auto hive_table = hms_client->getTable(db_name, table_name);
    HiveSchemaConverter converter(local_context, hive_table);
    //TODO(ExternalCatalog):: we set the metastore uri here to make the StorageCnchHive run. We need further modification.
    auto create_query_ast = converter.createQueryAST(configs->getString(key_hive_uri, "default"));
    ContextMutablePtr mutable_context = Context::createCopy(local_context->shared_from_this());
    auto ret = StorageFactory::instance().get(
        create_query_ast,
        "",
        mutable_context,
        mutable_context->getGlobalContext(),
        InterpreterCreateQuery::getColumnsDescription(*create_query_ast.columns_list->columns, mutable_context, false),
        InterpreterCreateQuery::getConstraintsDescription(create_query_ast.columns_list->constraints),
        false);
    auto create_query = serializeAST(create_query_ast);
    ret->setCreateTableSql(create_query);
    LOG_DEBUG(log, "{}.{}.{} create query: {}", name(), db_name, table_name, create_query);
    return ret;
}

UUID HiveExternalCatalog::getTableUUID(const std::string & db_name, const std::string & table_name)
{
    return UUIDHelpers::hashUUIDfromString(fmt::format("{}.{}.{}", catalog_name, db_name, table_name));
}
}
