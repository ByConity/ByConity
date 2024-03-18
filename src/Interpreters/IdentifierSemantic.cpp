/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Interpreters/IdentifierSemantic.h>

#include <Common/typeid_cast.h>

#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

#include <Parsers/ASTFunction.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AMBIGUOUS_COLUMN_NAME;
}

namespace
{

template <typename T>
std::optional<size_t> tryChooseTable(const ASTIdentifier & identifier, const std::vector<T> & tables,
                                     bool allow_ambiguous, bool column_match [[maybe_unused]] = false)
{
    using ColumnMatch = IdentifierSemantic::ColumnMatch;

    size_t best_table_pos = 0;
    auto best_match = ColumnMatch::NoMatch;
    size_t same_match = 0;

    for (size_t i = 0; i < tables.size(); ++i)
    {
        auto match = IdentifierSemantic::canReferColumnToTable(identifier, tables[i]);

        if constexpr (std::is_same_v<T, TableWithColumnNamesAndTypes>)
        {
            if (column_match && match == ColumnMatch::NoMatch && identifier.isShort() && tables[i].hasColumn(identifier.shortName()))
                match = ColumnMatch::ColumnName;
        }

        if (match != ColumnMatch::NoMatch)
        {
            if (match > best_match)
            {
                best_match = match;
                best_table_pos = i;
                same_match = 0;
            }
            else if (match == best_match)
                ++same_match;
        }
    }

    if ((best_match != ColumnMatch::NoMatch) && same_match)
    {
        if (!allow_ambiguous)
            throw Exception("Ambiguous column '" + identifier.name() + "'", ErrorCodes::AMBIGUOUS_COLUMN_NAME);
        best_match = ColumnMatch::Ambiguous;
        return {};
    }

    if (best_match != ColumnMatch::NoMatch)
        return best_table_pos;
    return {};
}

}

void IdentifierSemanticImpl::serialize(WriteBuffer & buf) const
{
    writeBinary(special, buf);
    writeBinary(can_be_alias, buf);
    writeBinary(covered, buf);
    if (membership)
    {
        writeBinary(true, buf);
        writeBinary(membership.value(), buf);
    }
    else
        writeBinary(false, buf);

    writeBinary(table, buf);
    writeBinary(legacy_compound, buf);
}

void IdentifierSemanticImpl::deserialize(ReadBuffer & buf)
{
    readBinary(special, buf);
    readBinary(can_be_alias, buf);
    readBinary(covered, buf);

    bool has_member;
    readBinary(has_member, buf);
    if (has_member){
        size_t member_tmp;
        readBinary(member_tmp, buf);
        membership = member_tmp;
    }

    readBinary(table, buf);
    readBinary(legacy_compound, buf);
}

std::optional<String> IdentifierSemantic::getColumnName(const ASTIdentifier & node)
{
    if (!node.semantic->special)
        return node.name();
    return {};
}

std::optional<String> IdentifierSemantic::getColumnName(const ASTPtr & ast)
{
    if (ast)
        if (const auto * id = ast->as<ASTIdentifier>())
            if (!id->semantic->special)
                return id->name();
    return {};
}

std::optional<ASTIdentifier> IdentifierSemantic::uncover(const ASTIdentifier & identifier)
{
    if (identifier.semantic->covered)
    {
        std::vector<String> name_parts = identifier.name_parts;
        return ASTIdentifier(std::move(name_parts));
    }
    return {};
}

void IdentifierSemantic::coverName(ASTIdentifier & identifier, const String & alias)
{
    identifier.setShortName(alias);
    identifier.semantic->covered = true;
}

bool IdentifierSemantic::canBeAlias(const ASTIdentifier & identifier)
{
    return identifier.semantic->can_be_alias;
}

bool IdentifierSemantic::isSpecial(const ASTIdentifier & identifier)
{
    return identifier.semantic->special;
}

void IdentifierSemantic::setMembership(ASTIdentifier & identifier, size_t table_pos)
{
    identifier.semantic->membership = table_pos;
    identifier.semantic->can_be_alias = false;
}

std::optional<size_t> IdentifierSemantic::getMembership(const ASTIdentifier & identifier)
{
    return identifier.semantic->membership;
}

std::optional<size_t> IdentifierSemantic::chooseTable(const ASTIdentifier & identifier, const std::vector<DatabaseAndTableWithAlias> & tables,
                                                      bool ambiguous)
{
    return tryChooseTable<DatabaseAndTableWithAlias>(identifier, tables, ambiguous);
}

std::optional<size_t> IdentifierSemantic::chooseTable(const ASTIdentifier & identifier, const TablesWithColumns & tables, bool ambiguous)
{
    return tryChooseTable<TableWithColumnNamesAndTypes>(identifier, tables, ambiguous);
}

std::optional<size_t> IdentifierSemantic::chooseTableColumnMatch(const ASTIdentifier & identifier, const TablesWithColumns & tables,
                                                                 bool ambiguous)
{
    return tryChooseTable<TableWithColumnNamesAndTypes>(identifier, tables, ambiguous, true);
}

std::optional<String> IdentifierSemantic::extractNestedName(const ASTIdentifier & identifier, const String & table_name)
{
    if (identifier.name_parts.size() == 3 && table_name == identifier.name_parts[0])
        return identifier.name_parts[1] + '.' + identifier.name_parts[2];
    else if (identifier.name_parts.size() == 2)
        return identifier.name_parts[0] + '.' + identifier.name_parts[1];
    return {};
}

std::pair<String, String> IdentifierSemantic::extractDatabaseAndTable(const ASTIdentifier & identifier)
{
    if (identifier.name_parts.size() > 2)
        throw Exception("Logical error: more than two components in table expression", ErrorCodes::LOGICAL_ERROR);

    if (identifier.name_parts.size() == 2)
        return { identifier.name_parts[0], identifier.name_parts[1] };
    return { "", identifier.name() };
}

bool IdentifierSemantic::doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & database, const String & table)
{
    size_t num_components = identifier.name_parts.size();
    if (num_components >= 3)
    {
        if (identifier.name_parts[1] == table)
        {
            if (identifier.name_parts[0] != database)
                return formatTenantDatabaseName(identifier.name_parts[0]) == database;
            return true;
        }
    }
    return false;
}

bool IdentifierSemantic::doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & table)
{
    size_t num_components = identifier.name_parts.size();
    if (num_components >= 2)
        return identifier.name_parts[0] == table;
    return false;
}

IdentifierSemantic::ColumnMatch IdentifierSemantic::canReferColumnToTable(const ASTIdentifier & identifier,
                                                                          const DatabaseAndTableWithAlias & db_and_table)
{
    /// database.table.column
    if (doesIdentifierBelongTo(identifier, db_and_table.database, db_and_table.table))
        return ColumnMatch::DbAndTable;

    /// alias.column
    if (doesIdentifierBelongTo(identifier, db_and_table.alias))
        return ColumnMatch::TableAlias;

    /// table.column
    if (doesIdentifierBelongTo(identifier, db_and_table.table))
    {
        if (!db_and_table.alias.empty())
            return ColumnMatch::AliasedTableName;
        else
            return ColumnMatch::TableName;
    }

    return ColumnMatch::NoMatch;
}

IdentifierSemantic::ColumnMatch IdentifierSemantic::canReferColumnToTable(const ASTIdentifier & identifier,
                                                                          const TableWithColumnNamesAndTypes & table_with_columns)
{
    return canReferColumnToTable(identifier, table_with_columns.table);
}

/// Strip qualifications from left side of column name.
/// Example: 'database.table.name' -> 'name'.
void IdentifierSemantic::setColumnShortName(ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table)
{
    auto match = IdentifierSemantic::canReferColumnToTable(identifier, db_and_table);
    size_t to_strip = 0;
    switch (match)
    {
        case ColumnMatch::TableName:
        case ColumnMatch::AliasedTableName:
        case ColumnMatch::TableAlias:
            to_strip = 1;
            break;
        case ColumnMatch::DbAndTable:
            to_strip = 2;
            break;
        default:
            break;
    }

    if (!to_strip)
        return;

    identifier.name_parts = std::vector<String>(identifier.name_parts.begin() + to_strip, identifier.name_parts.end());
    identifier.resetFullName();
}

void IdentifierSemantic::setColumnLongName(ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table)
{
    String prefix = db_and_table.getQualifiedNamePrefix();
    if (!prefix.empty())
    {
        prefix.resize(prefix.size() - 1); /// crop dot
        identifier.name_parts = {prefix, identifier.shortName()};
        identifier.resetFullName();
        identifier.semantic->table = prefix;
        identifier.semantic->legacy_compound = true;
    }
}

std::optional<size_t> IdentifierSemantic::getIdentMembership(const ASTIdentifier & ident, const std::vector<TableWithColumnNamesAndTypes> & tables)
{
    std::optional<size_t> table_pos = IdentifierSemantic::getMembership(ident);
    if (table_pos)
        return table_pos;
    return IdentifierSemantic::chooseTableColumnMatch(ident, tables, true);
}

std::optional<size_t>
IdentifierSemantic::getIdentsMembership(ASTPtr ast, const std::vector<TableWithColumnNamesAndTypes> & tables, const Aliases & aliases)
{
    auto idents = IdentifiersCollector::collect(ast);

    std::optional<size_t> result;
    for (const auto * ident : idents)
    {
        /// short name clashes with alias, ambiguous
        if (ident->isShort() && aliases.count(ident->shortName()))
            return {};
        const auto pos = getIdentMembership(*ident, tables);
        if (!pos)
            return {};
        /// identifiers from different tables
        if (result && *pos != *result)
            return {};
        result = pos;
    }
    return result;
}

/**
 * 1. target partition key expression: (func_1(a), func_2(b), func_3(c)) --> IdentifiersCollector --> (a, b, c) (parititon key identifier must be short name)
 * 
 * 2. mv select query : select func_7(func_a(d)) as a, b, func_8(func_b(e)) as c from ... --> aliases (a-> func_7(func_a(d)), c -> func_8(func_b(e)))
 * 
 * 3. source table partition key(func_a(d), func_b(e), b)
 * 
 * 4. generate expression actions : (func_1(func_7(`func_a(d)`)), func_2(`b`), func_3(func_8(`func_b(e)`))) 
 * 
 */
std::optional<size_t> IdentifierSemantic::getSourceTableAndColumnMapping(
    ASTPtr ast, const std::vector<TableWithColumnNamesAndTypes> & tables, Aliases & aliases, std::shared_ptr<ASTExpressionList> & collect_identifiers, bool is_recursion)
{
    auto idents = IdentifiersCollector::collect(ast);
    std::optional<size_t> result;
    Aliases empty_alias;
    for (const auto * ident : idents)
    {
        std::optional<size_t> pos;
        /// condition_1: if target table partition key is alias from base table to check the source table
        /// TODO: use name to compare maybe not enough
        if (aliases.count(ident->name()))
        {
            pos = getSourceTableAndColumnMapping(aliases[ident->name()], tables, empty_alias, collect_identifiers, true);
            collect_identifiers->children.emplace_back(aliases[ident->name()]);
        }
        /// condition_2: if target table partition key not in alias try to get source table with original column name
        else
        {
            pos = IdentifierSemantic::chooseTableColumnMatch(*ident, tables, true);
            if (!is_recursion)
                collect_identifiers->children.emplace_back(std::make_shared<ASTIdentifier>(*ident));
        }
            
        if (!pos)
            return {};
        /// identifiers from different tables
        if (result && *pos != *result)
            return {};
        
        result = pos;
    }

    return result;
}

IdentifiersCollector::ASTIdentifiers IdentifiersCollector::collect(const ASTPtr & node)
{
    IdentifiersCollector::Data ident_data;
    ConstInDepthNodeVisitor<IdentifiersCollector, true> ident_visitor(ident_data);
    ident_visitor.visit(node);
    return ident_data.idents;
}

bool IdentifiersCollector::needChildVisit(const ASTPtr &, const ASTPtr &)
{
    return true;
}

void IdentifiersCollector::visit(const ASTPtr & node, IdentifiersCollector::Data & data)
{
    if (const auto * ident = node->as<ASTIdentifier>())
        data.idents.push_back(ident);   
}

void RemoveQualifierVisitor::transform(ASTPtr & node)
{
    RemoveQualifierVisitor::Data ident_data;
    MutableInDepthNodeVisitor<RemoveQualifierVisitor, true> ident_visitor(ident_data);
    ident_visitor.visit(node);
}

bool RemoveQualifierVisitor::needChildVisit(ASTPtr &, ASTPtr &)
{
    return true;
}

void RemoveQualifierVisitor::visit(ASTPtr & node, RemoveQualifierVisitor::Data &)
{
    if (auto * ident = node->as<ASTIdentifier>())
    {
        if (ident->compound())
        {
            String table = ident->nameParts().back();
            ident->setShortName(table);
        }
    }
}


IdentifierMembershipCollector::IdentifierMembershipCollector(const ASTSelectQuery & select, ContextPtr context)
{
    if (ASTPtr with = select.with())
        QueryAliasesNoSubqueriesVisitor(aliases).visit(with);
    QueryAliasesNoSubqueriesVisitor(aliases).visit(select.select());

    const auto & settings = context->getSettingsRef();
    tables = getDatabaseAndTablesWithColumns(getTableExpressions(select), context,
                                             settings.asterisk_include_alias_columns,
                                             settings.asterisk_include_materialized_columns);
}

std::optional<size_t> IdentifierMembershipCollector::getIdentsMembership(ASTPtr ast) const
{
    return IdentifierSemantic::getIdentsMembership(ast, tables, aliases);
}

static void collectConjunctions(const ASTPtr & node, std::vector<ASTPtr> & members)
{
    if (const auto * func = node->as<ASTFunction>(); func && func->name == "and")
    {
        for (const auto & child : func->arguments->children)
            collectConjunctions(child, members);
        return;
    }
    members.push_back(node);
}

std::vector<ASTPtr> collectConjunctions(const ASTPtr & node)
{
    std::vector<ASTPtr> members;
    collectConjunctions(node, members);
    return members;
}

void IdentifierSemantic::setColumnTableName(ASTIdentifier & identifier, const String & table)
{
    identifier.name_parts = {table, identifier.shortName()};
    identifier.resetFullName();
    identifier.semantic->table = table;
    identifier.semantic->legacy_compound = true;
}

}
