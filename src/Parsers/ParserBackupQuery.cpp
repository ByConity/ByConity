#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ParserBackupQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <boost/range/algorithm_ext/erase.hpp>
#include <Common/assert_cast.h>


namespace DB
{

namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using ElementType = ASTBackupQuery::ElementType;

    bool parsePartitions(IParser::Pos & pos, Expected & expected, std::optional<ASTs> & partitions)
    {
        if (!ParserKeyword{"PARTITION"}.ignore(pos, expected) && !ParserKeyword{"PARTITIONS"}.ignore(pos, expected))
            return false;

        ASTs result;
        auto parse_list_element = [&] {
            ASTPtr ast;
            if (!ParserPartition{}.parse(pos, ast, expected))
                return false;
            result.push_back(ast);
            return true;
        };
        if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
            return false;

        partitions = std::move(result);
        return true;
    }


    bool parseExceptTables(
        IParser::Pos & pos,
        Expected & expected,
        const std::optional<String> & database_name,
        std::set<DatabaseAndTableName> & except_tables)
    {
        return IParserBase::wrapParseImpl(pos, [&] {
            if (!ParserKeyword{"EXCEPT TABLE"}.ignore(pos, expected) && !ParserKeyword{"EXCEPT TABLES"}.ignore(pos, expected))
                return false;

            std::set<DatabaseAndTableName> result;
            auto parse_list_element = [&] {
                DatabaseAndTableName table_name;
                if (parseDatabaseAndTableName(pos, expected, table_name.first, table_name.second))
                {
                    if (!table_name.first.empty() && database_name)
                        return database_name == table_name.first;
                    else if (table_name.first.empty())
                    {
                        if (database_name)
                            table_name.first = *database_name;
                        else
                            return false; 
                    }
                }

                result.emplace(std::move(table_name));
                return true;
            };
            if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
                return false;

            except_tables = std::move(result);
            return true;
        });
    }

    bool parseElement(IParser::Pos & pos, Expected & expected, Element & element)
    {
        return IParserBase::wrapParseImpl(pos, [&] {
            if (ParserKeyword{"TABLE"}.ignore(pos, expected))
            {
                element.type = ElementType::TABLE;
                if (!parseDatabaseAndTableName(pos, expected, element.database_name, element.table_name))
                    return false;

                element.new_database_name = element.database_name;
                element.new_table_name = element.table_name;
                if (ParserKeyword("AS").ignore(pos, expected))
                {
                    if (!parseDatabaseAndTableName(pos, expected, element.new_database_name, element.new_table_name))
                        return false;
                }

                parsePartitions(pos, expected, element.partitions);
                return true;
            }

            if (ParserKeyword{"DATABASE"}.ignore(pos, expected))
            {
                element.type = ElementType::DATABASE;

                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                element.database_name = getIdentifierName(ast);
                element.new_database_name = element.database_name;

                if (ParserKeyword("AS").ignore(pos, expected))
                {
                    ast = nullptr;
                    if (!ParserIdentifier{}.parse(pos, ast, expected))
                        return false;
                    element.new_database_name = getIdentifierName(ast);
                }

                parseExceptTables(pos, expected, element.database_name, element.except_tables);
                return true;
            }

            return false;
        });
    }

    bool parseElements(IParser::Pos & pos, Expected & expected, std::vector<Element> & elements)
    {
        return IParserBase::wrapParseImpl(pos, [&] {
            std::vector<Element> result;

            auto parse_element = [&] {
                Element element;
                if (parseElement(pos, expected, element))
                {
                    result.emplace_back(std::move(element));
                    return true;
                }
                return false;
            };

            if (!ParserList::parseUtil(pos, expected, parse_element, false))
                return false;

            elements = std::move(result);
            return true;
        });
    }

    bool parseBackupDisk(IParser::Pos & pos, Expected & expected, ASTPtr & backup_name)
    {
        return ParserIdentifierWithOptionalParameters{}.parse(pos, backup_name, expected);
    }
}


bool ParserBackupQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Kind kind;
    if (ParserKeyword{"BACKUP"}.ignore(pos, expected))
        kind = Kind::BACKUP;
    else if (ParserKeyword{"RESTORE"}.ignore(pos, expected))
        kind = Kind::RESTORE;
    else
        return false;

    std::vector<Element> elements;
    if (!parseElements(pos, expected, elements))
        return false;

    if (!ParserKeyword{(kind == Kind::BACKUP) ? "TO" : "FROM"}.ignore(pos, expected))
        return false;

    ASTPtr backup_disk;
    if (!parseBackupDisk(pos, expected, backup_disk))
        return false;

    ASTPtr settings;
    /// Read SETTINGS if they are defined
    if (ParserKeyword{"SETTINGS"}.ignore(pos, expected))
    {
        /// Settings are written like SET query, so parse them with ParserSetQuery
        ParserSetQuery parser_settings(true);
        if (!parser_settings.parse(pos, settings, expected))
            return false;
    }

    auto query = std::make_shared<ASTBackupQuery>();
    node = query;

    query->kind = kind;
    query->elements = std::move(elements);
    if (backup_disk)
        query->set(query->backup_disk, backup_disk);
    query->settings = std::move(settings);

    return true;
}

}
