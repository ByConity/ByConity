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
    , log(getLogger("DatabaseExternalHive(" + database_name + ")"))
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
        auto now = time(nullptr);
        {
            std::shared_lock rd{cache_mutex};
            auto it = cache.find(table_name);
            if (it != cache.end())
            {
                if (std::get<1>(it->second) + static_cast<int64_t>(context_->getSettingsRef().hive_cache_expire_time.value) >= now)
                {
                    return std::get<0>(it->second);
                }
            }
        }

        auto res = hive_catalog->getTable(getDatabaseName(), table_name, context_);
        if (res)
        {
            std::lock_guard wr{cache_mutex};
            cache.emplace(table_name, std::make_tuple(res, now));
            return res;
        }
    }
    catch ([[maybe_unused]] const ApacheHive::NoSuchObjectException & e)
    {
        return nullptr;
    }
    return nullptr;
}

class FakeDatabaseTablesIterator : public IDatabaseTablesIterator
{
public:
    FakeDatabaseTablesIterator(const std::string & name, const std::vector<StoragePtr> & all_tables_) : IDatabaseTablesIterator(name), all_tables(all_tables_) { }
    FakeDatabaseTablesIterator(const std::string & name, std::vector<StoragePtr> && all_tables_) : IDatabaseTablesIterator(name), all_tables(std::move(all_tables_)) { }
    void next() override { ++index;}
    bool isValid() const override { return index < all_tables.size(); }
    const String & name() const override {return database_name;}
    const StoragePtr & table() const override { return all_tables[index]; }


private:
    std::vector<StoragePtr> all_tables;
    size_t index = 0 ;
};

DatabaseTablesIteratorPtr DatabaseExternalHive::getTablesIterator(ContextPtr context_, [[maybe_unused]] const FilterByNameFunction & filter_by_table_name)
{
    auto all_table_name = hive_catalog->listTableNames(getDatabaseName());
    std::vector<StoragePtr> all_tables;
    for(const auto & name : all_table_name)
    {
        if(!filter_by_table_name || filter_by_table_name(name))
        {
            all_tables.push_back(tryGetTable(name, context_));
        }
    }
    return std::make_unique<FakeDatabaseTablesIterator>(getDatabaseName(), std::move(all_tables));
}

bool DatabaseExternalHive::empty() const
{
    return false;
}


ASTPtr DatabaseExternalHive::getCreateDatabaseQuery() const
{
    throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "getCreateDatabaseQuery shall never be called for ExternalHive database.");
}

void DatabaseExternalHive::shutdown()
{
}

ASTPtr DatabaseExternalHive::getCreateTableQueryImpl(const String & name, ContextPtr local_context, bool throw_on_error) const
{
    StoragePtr storage = nullptr;
    try
    {
        storage = tryGetTable(name, local_context);
    }
    catch (...)
    {
        LOG_DEBUG(
            log,
            "Fail to try to get create query for external table {} in database {} query id {}",
            name,
            getDatabaseName(),
            local_context->getCurrentQueryId());
    }

    if (storage == nullptr && throw_on_error)
    {
        throw Exception("Table " + getDatabaseName() + "." + name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
    }
    if (storage == nullptr)
        return nullptr;

    String create_table_query = storage->getCreateTableSql();
    ParserCreateQuery p_create_query;
    ASTPtr ast{};
    try
    {
        ast = parseQuery(
            p_create_query,
            create_table_query,
            local_context->getSettingsRef().max_query_size,
            local_context->getSettingsRef().max_parser_depth);
    }
    catch (...)
    {
        if (throw_on_error)
            throw;
        else
            LOG_DEBUG(
                log,
                "Fail to parseQuery for external table {} in database {} query id {}, create query {}",
                name,
                getDatabaseName(),
                local_context->getCurrentQueryId(),
                create_table_query);
    }

    return ast;
}
}
