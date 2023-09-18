#include <memory>
#include <hive_metastore_types.h>
#include <Databases/DatabaseExternalHive.h>
#include <Dictionaries/DictionaryStructure.h>
#include <ExternalCatalog/IExternalCatalogMgr.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageDictionary.h>
#include "Common/Exception.h"
#include "Common/typeid_cast.h"
#include <common/logger_useful.h>
#include "Databases/IDatabase.h"
namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_DICTIONARY_QUERY;
}


DatabaseExternalHive::DatabaseExternalHive(const String & catalog_, const String & name_, ContextPtr context_)
    : IDatabase(name_)
    , WithContext(context_->getGlobalContext())
    , hive_catalog_name(catalog_)
    , log(&Poco::Logger::get("DatabaseExternalHive(" + database_name + ")"))
{
    // std::optional<String> hive_catalog_opt;
    // std::optional<String> hive_db_opt;
    // std::tie(hive_catalog_opt, hive_db_opt) = getCatalogNameAndDatabaseName(name_);
    // if(hive_catalog_opt->empty()){
    //     throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} does not contain catalog information", name_);
    // }
    // if(hive_db_opt->empty()){
    //     throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} does not contain database information", name_);
    // }
    // hive_catalog_opt = hive_db_opt
    // hive_db_name = hive_db_opt.value();
    hive_catalog = ExternalCatalog::Mgr::instance().getCatalog(hive_catalog_name);
}


bool DatabaseExternalHive::isTableExist(const String & table_name, ContextPtr context_) const
{
    return hive_catalog->isTableExist(getDatabaseName(), table_name, context_);
}

StoragePtr DatabaseExternalHive::tryGetTable(const String & table_name, ContextPtr context_) const
{
    try
    {
        {
            std::shared_lock rd{cache_mutex};
            auto it = cache.find(table_name);
            if (it != cache.end())
                return it->second;
        }
        auto res = hive_catalog->getTable(getDatabaseName(), table_name, context_);
        if (res)
        {
            std::lock_guard wr{cache_mutex};
            cache.emplace(table_name, res);
            return res;
        }
    }
    catch ([[maybe_unused]] const ApacheHive::NoSuchObjectException & e)
    {
        return nullptr;
    }
}

DatabaseTablesIteratorPtr
DatabaseExternalHive::getTablesIterator(ContextPtr, [[maybe_unused]] const FilterByNameFunction & filter_by_table_name)
{
    return std::make_unique<EmptyDatabaseTablesIterator>(getDatabaseName());
}

bool DatabaseExternalHive::empty() const
{
    return false;
}


ASTPtr DatabaseExternalHive::getCreateDatabaseQuery() const
{
    throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "getCreateDatabaseQuery shall never be called for ExternalHive database.");
    String query;
    {
        WriteBufferFromString buffer(query);
        buffer << "CREATE DATABASE " << backQuoteIfNeed(getDatabaseName()) << " ENGINE = ExternalHive";
    }
    auto settings = getContext()->getSettingsRef();
    ParserCreateQuery parser(ParserSettings::valueOf(settings));
    return parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth);
}

void DatabaseExternalHive::shutdown()
{
}

}
