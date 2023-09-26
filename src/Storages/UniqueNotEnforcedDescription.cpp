#include <Parsers/ASTUniqueNotEnforcedDeclaration.h>
#include <Storages/UniqueNotEnforcedDescription.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <Core/Defines.h>
#include <Parsers/ASTUniqueNotEnforcedDeclaration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String UniqueNotEnforcedDescription::toString() const
{
    if (unique.empty())
        return {};

    ASTExpressionList list;
    for (const auto & unique_key : unique)
        list.children.push_back(unique_key);

    return serializeAST(list, true);
}

UniqueNotEnforcedDescription UniqueNotEnforcedDescription::parse(const String & str)
{
    if (str.empty())
        return {};

    UniqueNotEnforcedDescription res;
    ParserForeignKeyDeclarationList parser(ParserSettings::CLICKHOUSE);
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    for (const auto & unique_key : list->children)
        res.unique.push_back(unique_key);

    return res;
}

const std::vector<ASTPtr> & UniqueNotEnforcedDescription::getUnique() const
{
    return unique;
}


UniqueNotEnforcedDescription::UniqueNotEnforcedDescription(const ASTs & unique_keys_) : unique(unique_keys_)
{
}

UniqueNotEnforcedDescription::UniqueNotEnforcedDescription(const UniqueNotEnforcedDescription & other)
{
    unique.reserve(other.unique.size());
    for (const auto & unique_key : other.unique)
        unique.emplace_back(unique_key->clone());
}

UniqueNotEnforcedDescription & UniqueNotEnforcedDescription::operator=(const UniqueNotEnforcedDescription & other)
{
    unique.resize(other.unique.size());
    for (size_t i = 0; i < unique.size(); ++i)
        unique[i] = other.unique[i]->clone();
    return *this;
}

UniqueNotEnforcedDescription::UniqueNotEnforcedDescription(UniqueNotEnforcedDescription && other) noexcept : unique(std::move(other.unique))
{
}

UniqueNotEnforcedDescription & UniqueNotEnforcedDescription::operator=(UniqueNotEnforcedDescription && other) noexcept
{
    unique = std::move(other.unique);

    return *this;
}

std::vector<Names> UniqueNotEnforcedDescription::getUniqueNames() const
{
    std::vector<Names> result;
    for (const auto & unique_key : unique)
    {
        result.push_back(Names{});
        auto list = unique_key->as<ASTUniqueNotEnforcedDeclaration &>().column_names->as<ASTExpressionList &>();
        for (const auto & child : list.children)
            result.back().emplace_back(child->as<ASTIdentifier &>().name());
    }
    return result;
}

}
