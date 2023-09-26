#include <Parsers/ASTForeignKeyDeclaration.h>
#include <Storages/ForeignKeysDescription.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Optimizer/DataDependency/ForeignKeysTuple.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <Core/Defines.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String ForeignKeysDescription::toString() const
{
    if (foreign_keys.empty())
        return {};

    ASTExpressionList list;
    for (const auto & foreign_key : foreign_keys)
        list.children.push_back(foreign_key);

    return serializeAST(list, true);
}

ForeignKeysDescription ForeignKeysDescription::parse(const String & str)
{
    if (str.empty())
        return {};

    ForeignKeysDescription res;
    ParserForeignKeyDeclarationList parser(ParserSettings::CLICKHOUSE);
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    for (const auto & foreign_key : list->children)
        res.foreign_keys.push_back(foreign_key);

    return res;
}

const std::vector<ASTPtr> & ForeignKeysDescription::getForeignKeys() const
{
    return foreign_keys;
}


ForeignKeysDescription::ForeignKeysDescription(const ASTs & foreign_keys_) : foreign_keys(foreign_keys_)
{
}

ForeignKeysDescription::ForeignKeysDescription(const ForeignKeysDescription & other)
{
    foreign_keys.reserve(other.foreign_keys.size());
    for (const auto & foreign_key : other.foreign_keys)
        foreign_keys.emplace_back(foreign_key->clone());
}

ForeignKeysDescription & ForeignKeysDescription::operator=(const ForeignKeysDescription & other)
{
    foreign_keys.resize(other.foreign_keys.size());
    for (size_t i = 0; i < foreign_keys.size(); ++i)
        foreign_keys[i] = other.foreign_keys[i]->clone();
    return *this;
}

ForeignKeysDescription::ForeignKeysDescription(ForeignKeysDescription && other) noexcept : foreign_keys(std::move(other.foreign_keys))
{
}

ForeignKeysDescription & ForeignKeysDescription::operator=(ForeignKeysDescription && other) noexcept
{
    foreign_keys = std::move(other.foreign_keys);

    return *this;
}

ForeignKeysTuple ForeignKeysDescription::getForeignKeysTuple() const
{
    ForeignKeysTuple ref_names;
    for (const auto & foreign_key : foreign_keys)
    {
        ASTExpressionList * column1 = foreign_key->as<ASTForeignKeyDeclaration &>().column_names->as<ASTExpressionList>();
        String ref_table_name = foreign_key->as<ASTForeignKeyDeclaration &>().ref_table_name;
        ASTExpressionList * column2 = foreign_key->as<ASTForeignKeyDeclaration &>().ref_column_names->as<ASTExpressionList>();
        if (column1 && column2)
        {
            ref_names.emplace_back(
                column1->children[0]->as<ASTIdentifier &>().name(), ref_table_name, column2->children[0]->as<ASTIdentifier &>().name());
        }
        else
        {
            throw Exception("unexpected foreign key ast_column_names types!", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
    return ref_names;
}
}
