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

#pragma once

#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/DumpASTNode.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/Context.h>
#include <set>

namespace DB
{

/// Visitors consist of functions with unified interface 'void visit(Casted & x, ASTPtr & y)', there x is y, successfully casted to Casted.
/// Both types and function could have const specifiers. The second argument is used by visitor to replaces AST node (y) if needed.

/// Visits AST nodes, add default database to tables if not set. There's different logic for DDLs and selects.
class AddDefaultDatabaseVisitor
{
public:
    struct Data
    {
        std::set<String> subqueries;
    };

    explicit AddDefaultDatabaseVisitor(
        ContextPtr context_,
        const String & database_name_,
        bool only_replace_current_database_function_ = false,
        bool only_replace_in_join_ = false,
        WriteBuffer * ostr_ = nullptr)
        : context(context_)
        , database_name(database_name_)
        , only_replace_current_database_function(only_replace_current_database_function_)
        , only_replace_in_join(only_replace_in_join_)
        , visit_depth(0)
        , ostr(ostr_)
    {
		if (!context->isGlobalContext())
        {
            for (const auto & [table_name, _ /* storage */] : context->getExternalTables())
            {
                external_tables.insert(table_name);
            }
        }
    }

    void visitDDL(ASTPtr & ast) const
    {
        visitDDLChildren(ast);

        if (!tryVisitDynamicCast<ASTQueryWithTableAndOutput>(ast) &&
            !tryVisitDynamicCast<ASTRenameQuery>(ast) &&
            !tryVisitDynamicCast<ASTFunction>(ast))
        {}
    }

    void visit(ASTPtr & ast) const
    {
        Data data;
        visit(ast, data);
    }

    void visit(ASTSelectQuery & select) const
    {
        Data data;
        visit(select, data);
    }
    void visit(ASTSelectWithUnionQuery & select) const
    {
        Data data;
        visit(select, data);
    }

private:
    ContextPtr context;

    const String database_name;
    std::set<String> external_tables;

    bool only_replace_current_database_function = false;
    bool only_replace_in_join = false;
    mutable size_t visit_depth;
    WriteBuffer * ostr;

    void visit(ASTPtr & ast, Data & data) const
    {
        if (!tryVisit<ASTSelectQuery>(ast, data) && !tryVisit<ASTSelectWithUnionQuery>(ast, data) && !tryVisit<ASTFunction>(ast, data))
            visitChildren(*ast, data);
    }

    void visit(ASTSelectQuery & select, Data & data) const
    {
        ASTPtr unused;
        visit(select, unused, data);
    }

    void visit(ASTSelectWithUnionQuery & select, Data & data) const
    {
        ASTPtr unused;
        visit(select, unused, data);
    }

    void visit(ASTSelectWithUnionQuery & select, ASTPtr &, Data & data) const
    {
        for (auto & child : select.list_of_selects->children)
        {
            if (child->as<ASTSelectQuery>())
                tryVisit<ASTSelectQuery>(child, data);
            else if (child->as<ASTSelectIntersectExceptQuery>())
                tryVisit<ASTSelectIntersectExceptQuery>(child, data);
        }
    }

    void visit(ASTSelectQuery & select, ASTPtr &, Data & data) const
    {
        std::optional<Data> new_data;
        if (auto with = select.with())
        {
            for (auto & child : with->children)
            {
                visit(child, new_data ? *new_data : data);
                if (auto * ast_with_elem = child->as<ASTWithElement>())
                {
                    if (!new_data)
                        new_data = data;
                    new_data->subqueries.emplace(ast_with_elem->name);
                }
            }
        }

        if (select.tables())
            tryVisit<ASTTablesInSelectQuery>(select.refTables(), new_data ? *new_data : data);

        visitChildren(select, new_data ? *new_data : data);
    }

    void visit(ASTSelectIntersectExceptQuery & select, ASTPtr &, Data & data) const
    {
        for (auto & child : select.getListOfSelects())
        {
            if (child->as<ASTSelectQuery>())
                tryVisit<ASTSelectQuery>(child, data);
            else if (child->as<ASTSelectIntersectExceptQuery>())
                tryVisit<ASTSelectIntersectExceptQuery>(child, data);
            else if (child->as<ASTSelectWithUnionQuery>())
                tryVisit<ASTSelectWithUnionQuery>(child, data);
        }
    }

    void visit(ASTTablesInSelectQuery & tables, ASTPtr &, Data & data) const
    {
        for (auto & child : tables.children)
            tryVisit<ASTTablesInSelectQueryElement>(child, data);
    }

    void visit(ASTTablesInSelectQueryElement & tables_element, ASTPtr &, Data & data) const
    {
        if (only_replace_in_join && !tables_element.table_join)
            return;

        if (tables_element.table_expression)
            tryVisit<ASTTableExpression>(tables_element.table_expression, data);
    }

    void visit(ASTTableExpression & table_expression, ASTPtr &, Data & data) const
    {
        if (table_expression.database_and_table_name)
            tryVisit<ASTTableIdentifier>(table_expression.database_and_table_name, data);
    }

    void visit(const ASTTableIdentifier & identifier, ASTPtr & ast, Data & data) const
    {
		/// Already has database.
        if (identifier.compound())
            return;

        /// There is temporary table with such name, should not be rewritten.
        if (external_tables.count(identifier.shortName()))
			return;

        if (data.subqueries.count(identifier.shortName()))
            return;

        auto qualified_identifier = std::make_shared<ASTTableIdentifier>(database_name, identifier.name());
        if (!identifier.alias.empty())
            qualified_identifier->setAlias(identifier.alias);
        ast = qualified_identifier;
    }

    void visit(ASTFunction & function, ASTPtr &, Data & data) const
    {
        bool is_operator_in = false;
        for (const auto * name : {"in", "notIn", "globalIn", "globalNotIn"})
        {
            if (function.name == name)
            {
                is_operator_in = true;
                break;
            }
        }

        for (auto & child : function.children)
        {
            if (child.get() == function.arguments.get())
            {
                for (size_t i = 0; i < child->children.size(); ++i)
                {
                    if (is_operator_in && i == 1)
                    {
                        /// XXX: for some unknown reason this place assumes that argument can't be an alias,
                        ///      like in the similar code in `MarkTableIdentifierVisitor`.
                        if (auto * identifier = child->children[i]->as<ASTIdentifier>())
                            child->children[i] = identifier->createTable();

                        /// Second argument of the "in" function (or similar) may be a table name or a subselect.
                        /// Rewrite the table name or descend into subselect.
                        if (!tryVisit<ASTTableIdentifier>(child->children[i], data))
                            visit(child->children[i], data);
                    }
                    else
                        visit(child->children[i], data);
                }
            }
            else
                visit(child, data);
        }
    }

    void visitChildren(IAST & ast, Data & data) const
    {
        for (auto & child : ast.children)
            visit(child, data);
    }

    template <typename T>
    bool tryVisit(ASTPtr & ast, Data & data) const
    {
        if (T * t = typeid_cast<T *>(ast.get()))
        {
            DumpASTNode dump(*ast, ostr, visit_depth, "addDefaultDatabaseName");
            visit(*t, ast, data);
            return true;
        }
        return false;
    }


    void visitDDL(ASTQueryWithTableAndOutput & node, ASTPtr &) const
    {
        if (only_replace_current_database_function)
            return;

        if (node.database.empty())
            node.database = database_name;
    }

    void visitDDL(ASTRenameQuery & node, ASTPtr &) const
    {
        if (only_replace_current_database_function)
            return;

        for (ASTRenameQuery::Element & elem : node.elements)
        {
            if (elem.from.database.empty())
                elem.from.database = database_name;
            if (elem.to.database.empty())
                elem.to.database = database_name;
        }
    }

    void visitDDL(ASTFunction & function, ASTPtr & node) const
    {
        if (function.name == "currentDatabase")
        {
            node = std::make_shared<ASTLiteral>(database_name);
            return;
        }
    }

    void visitDDLChildren(ASTPtr & ast) const
    {
        for (auto & child : ast->children)
            visitDDL(child);
    }

    template <typename T>
    bool tryVisitDynamicCast(ASTPtr & ast) const
    {
        if (T * t = dynamic_cast<T *>(ast.get()))
        {
            visitDDL(*t, ast);
            return true;
        }
        return false;
    }
};

}
