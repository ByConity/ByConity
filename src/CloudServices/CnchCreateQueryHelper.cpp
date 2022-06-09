#include <CloudServices/CnchCreateQueryHelper.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>


namespace DB
{

std::shared_ptr<ASTCreateQuery> getASTCreateQueryFromString(const String & query)
{
    ParserCreateQuery parser_create;
    return std::dynamic_pointer_cast<ASTCreateQuery>(parseQuery(parser_create, query, 0, 0));
}

void replaceCnchWithCloud(ASTCreateQuery & create_query, const String & new_table_name, const String & cnch_db, const String & cnch_table)
{
    if (!new_table_name.empty())
        create_query.table = new_table_name;

    auto * storage = create_query.storage;

    auto engine = std::make_shared<ASTFunction>();
    if (auto pos = storage->engine->name.find("Cnch"); pos != std::string::npos)
        engine->name = String(storage->engine->name).replace(pos, strlen("Cnch"), "Cloud");

    engine->arguments = std::make_shared<ASTExpressionList>();
    engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(cnch_db));
    engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(cnch_table));
    // if (storage->unique_key && storage->engine->arguments && storage->engine->arguments->children.size())
    //     /// NOTE: Used to pass the version column for unique table here.
    //     engine->arguments->children.push_back(storage->engine->arguments->children[0]);
    storage->set(storage->engine, engine);
}

void modifyOrAddSetting(ASTSetQuery & set_query, const String & name, Field value)
{
    for (auto & change : set_query.changes)
    {
        if (change.name == name)
        {
            change.value = std::move(value);
            return;
        }
    }
    set_query.changes.emplace_back(name, std::move(value));
}

void modifyOrAddSetting(ASTCreateQuery & create_query, const String & name, Field value)
{
    auto * storage = create_query.storage;

    if (!storage->settings)
    {
        storage->set(storage->settings, std::make_shared<ASTSetQuery>());
        storage->settings->is_standalone = false;
    }

    modifyOrAddSetting(*storage->settings, name, std::move(value));
}
}
