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

ExternalLoaderCnchCatalogRepository::ExternalLoaderCnchCatalogRepository(ContextPtr context_)
    : context{std::move(context_)},
      catalog{context->getCnchCatalog()}
{}

std::string ExternalLoaderCnchCatalogRepository::getName() const
{
    return "CnchCatalogRepository";
}

std::set<std::string> ExternalLoaderCnchCatalogRepository::getAllLoadablesDefinitionNames()
{
    std::set<std::string> res;
    Catalog::Catalog::DataModelDictionaries dictionary_model = catalog->getAllDictionaries();
    std::for_each(dictionary_model.begin(), dictionary_model.end(),
        [& res] (const Protos::DataModelDictionary & d)
        {
            const UInt64 & status = d.status();
            if (Status::isDeleted(status) || Status::isDetached(status))
                return;
            res.insert(toString(RPCHelpers::createUUID(d.uuid())));
        });

    return res;
}

bool ExternalLoaderCnchCatalogRepository::exists(const std::string & loadable_definition_name)
{
    Catalog::Catalog::DataModelDictionaries dictionary_models = catalog->getAllDictionaries();
    for (const Protos::DataModelDictionary & d : dictionary_models)
    {
        const UInt64 & status = d.status();
        if (Status::isDeleted(status) || Status::isDetached(status))
            continue;

        if (toString(RPCHelpers::createUUID(d.uuid())) == loadable_definition_name)
            return true;
    }

    return false;
}

Poco::Timestamp ExternalLoaderCnchCatalogRepository::getUpdateTime(const std::string & loadable_definition_name)
{
    Catalog::Catalog::DataModelDictionaries dictionary_models = catalog->getAllDictionaries();
    for (const Protos::DataModelDictionary & d : dictionary_models)
    {
        const UInt64 & status = d.status();
        if (Status::isDeleted(status) || Status::isDetached(status))
            continue;

        if (toString(RPCHelpers::createUUID(d.uuid())) == loadable_definition_name)
            return d.last_modification_time();
    }

    throw Exception("dictionary uuid not found : " + loadable_definition_name, ErrorCodes::LOGICAL_ERROR);
}

LoadablesConfigurationPtr ExternalLoaderCnchCatalogRepository::load(const std::string & loadable_definition_name)
{
    Catalog::Catalog::DataModelDictionaries dictionary_models = catalog->getAllDictionaries();
    for (const Protos::DataModelDictionary & d : dictionary_models)
    {
        const UInt64 & status = d.status();
        if (Status::isDeleted(status) || Status::isDetached(status))
            continue;

        if (toString(RPCHelpers::createUUID(d.uuid())) == loadable_definition_name)
        {
            ASTPtr ast = Catalog::CatalogFactory::getCreateDictionaryByDataModel(d);
            const ASTCreateQuery & create_query = ast->as<ASTCreateQuery &>();
            DictionaryConfigurationPtr abstract_dictionary_configuration = getDictionaryConfigurationFromAST(create_query, context, d.database());
            abstract_dictionary_configuration->setBool("is_cnch_dictionary", true);

            return abstract_dictionary_configuration;
        }
    }

    throw Exception("dictionary uuid not found : " + loadable_definition_name, ErrorCodes::LOGICAL_ERROR);
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
    if (context->getCnchCatalog()->isDictionaryExists(storage_id.getDatabaseName(), storage_id.getTableName()))
    {
        Protos::DataModelDictionary d =
            context->getCnchCatalog()->getDictionary(storage_id.getDatabaseName(), storage_id.getTableName());

        return RPCHelpers::createUUID(d.uuid());
    }
    else
        return {};
}
}
