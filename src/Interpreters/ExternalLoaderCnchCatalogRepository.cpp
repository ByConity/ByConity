#include "ExternalLoaderCnchCatalogRepository.h"
#include <Interpreters/Context.h>
#include <Catalog/Catalog.h>
#include <Common/Status.h>
#include <Catalog/CatalogFactory.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

CnchCatalogDictionaryCache::CnchCatalogDictionaryCache(ContextPtr context_)
    : context{context_},
      catalog{context->getCnchCatalog()}
{
    loadFromCatalog();
}

std::unordered_map<String, DB::Protos::DataModelDictionary> fetchCacheDataFromCatalog(Catalog::Catalog * catalog)
{
    Catalog::Catalog::DataModelDictionaries all = catalog->getAllDictionaries();
    std::unordered_map<String, DB::Protos::DataModelDictionary> res;
    std::for_each(all.begin(), all.end(),
        [& res] (const Protos::DataModelDictionary & d)
        {
            const UInt64 & status = d.status();
            if (Status::isDeleted(status) || Status::isDetached(status))
                return;
            String uuid_str = toString(RPCHelpers::createUUID(d.uuid()));
            res.insert(std::make_pair(uuid_str, d));
        });
    return res;
}

void CnchCatalogDictionaryCache::loadFromCatalog()
{
    std::unordered_map<String, DB::Protos::DataModelDictionary> new_data =
        fetchCacheDataFromCatalog(catalog.get());
    std::lock_guard lock{data_mutex};
    std::swap(new_data, data);
}

std::set<std::string> CnchCatalogDictionaryCache::getAllUUIDString() const
{
    std::set<std::string> res;
    std::lock_guard lock{data_mutex};
    std::transform(data.begin(), data.end(), std::inserter(res, res.end()),
        [] (const std::pair<String, DB::Protos::DataModelDictionary> & p)
        {
            return p.first;
        }
    );
    return res;
}

bool CnchCatalogDictionaryCache::exists(const String & uuid_str) const
{
    std::lock_guard lock{data_mutex};
    return data.contains(uuid_str);
}

Poco::Timestamp CnchCatalogDictionaryCache::getUpdateTime(const String & uuid_str) const
{
    {
        std::lock_guard lock{data_mutex};
        auto it = data.find(uuid_str);
        if (it != data.end())
            return it->second.last_modification_time();
    }
    throw Exception("dictionary with uuid not found : " + uuid_str, ErrorCodes::LOGICAL_ERROR);
}

LoadablesConfigurationPtr CnchCatalogDictionaryCache::load(const String & uuid_str) const
{
    std::optional<DB::Protos::DataModelDictionary> d;
    {
        std::lock_guard lock{data_mutex};
        auto it = data.find(uuid_str);
        if (it != data.end())
            d = it->second;
    }

    if (!d)
        throw Exception("dictionary with uuid not found : " + uuid_str, ErrorCodes::LOGICAL_ERROR);

    ASTPtr ast = Catalog::CatalogFactory::getCreateDictionaryByDataModel(*d);
    const ASTCreateQuery & create_query = ast->as<ASTCreateQuery &>();
    DictionaryConfigurationPtr abstract_dictionary_configuration =
        getDictionaryConfigurationFromAST(create_query, context, d->database());
    abstract_dictionary_configuration->setBool("is_cnch_dictionary", true);

    return abstract_dictionary_configuration;
}

std::optional<UUID> CnchCatalogDictionaryCache::findUUID(const StorageID & storage_id) const
{
    std::lock_guard lock{data_mutex};
    for (const auto & p : data)
    {
        const DB::Protos::DataModelDictionary & d = p.second;
        if ((d.database() != storage_id.getDatabaseName()) ||
            (d.name() != storage_id.getTableName()))
            continue;

        return RPCHelpers::createUUID(d.uuid());
    }

    return {};
}

ExternalLoaderCnchCatalogRepository::ExternalLoaderCnchCatalogRepository(ContextPtr context_)
    : cache{context_->getCnchCatalogDictionaryCache()},
      catalog{context_->getCnchCatalog()}
{}

std::string ExternalLoaderCnchCatalogRepository::getName() const
{
    return "CnchCatalogRepository";
}

std::set<std::string> ExternalLoaderCnchCatalogRepository::getAllLoadablesDefinitionNames()
{
    cache.loadFromCatalog();
    return cache.getAllUUIDString();
}

bool ExternalLoaderCnchCatalogRepository::exists(const std::string & loadable_definition_name)
{
    return cache.exists(loadable_definition_name);
}

Poco::Timestamp ExternalLoaderCnchCatalogRepository::getUpdateTime(const std::string & loadable_definition_name)
{
    return cache.getUpdateTime(loadable_definition_name);
}

LoadablesConfigurationPtr ExternalLoaderCnchCatalogRepository::load(const std::string & loadable_definition_name)
{
    return cache.load(loadable_definition_name);
}

StorageID ExternalLoaderCnchCatalogRepository::parseStorageID(const std::string & loadable_definition_name)
{
    constexpr size_t max_size = 10000;
    constexpr unsigned long max_parser_depth = 10;

    Tokens tokens(loadable_definition_name.data(), loadable_definition_name.data() + loadable_definition_name.size(), max_size);
    IParser::Pos pos(tokens, max_parser_depth);
    Expected expected;

    ParserIdentifier name_p;
    ParserToken s_dot(TokenType::Dot);
    ASTPtr database;
    ASTPtr table;

    if (!name_p.parse(pos, table, expected))
        throw Exception("Failed to parse table id: " + loadable_definition_name, ErrorCodes::LOGICAL_ERROR);

    if (!s_dot.ignore(pos, expected))
        throw Exception("Failed to parse table id: " + loadable_definition_name, ErrorCodes::LOGICAL_ERROR);

    database = table;
    if (!name_p.parse(pos, table, expected))
        throw Exception("Failed to parse table id: " + loadable_definition_name, ErrorCodes::LOGICAL_ERROR);

    return StorageID{getIdentifierName(database), getIdentifierName(table)};
}

std::optional<UUID> ExternalLoaderCnchCatalogRepository::resolveDictionaryName(const std::string & name, ContextPtr context)
{
    StorageID storage_id = ExternalLoaderCnchCatalogRepository::parseStorageID(name);
    const CnchCatalogDictionaryCache & cache = context->getCnchCatalogDictionaryCache();
    return cache.findUUID(storage_id);
}
}
