/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <CloudServices/CnchCreateQueryHelper.h>

#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTForeignKeyDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUniqueNotEnforcedDeclaration.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include "Storages/ColumnsDescription.h"
#include "Storages/ForeignKeysDescription.h"
#include "Storages/UniqueNotEnforcedDescription.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_COLUMN;
    extern const int INCORRECT_QUERY;
}

std::shared_ptr<ASTCreateQuery> getASTCreateQueryFromString(const String & query, const ContextPtr & context)
{
    ParserCreateQuery parser_create;
    const auto & settings = context->getSettingsRef();
    auto create_ast = std::dynamic_pointer_cast<ASTCreateQuery>(parseQuery(parser_create, query, settings.max_query_size, settings.max_parser_depth));
    create_ast->attach = true;
    create_ast->create = false;

    if (create_ast->select && create_ast->isView())
        ApplyWithSubqueryVisitor().visit(*create_ast->select);

    return create_ast;
}

std::shared_ptr<ASTCreateQuery> getASTCreateQueryFromStorage(const IStorage & storage, const ContextPtr & context)
{
    return getASTCreateQueryFromString(storage.getCreateTableSql(), context);
}

StoragePtr createStorageFromQuery(ASTCreateQuery & create_query, ContextMutablePtr context)
{
    ColumnsDescription columns;
    IndicesDescription indices;
    ConstraintsDescription constraints;
    ForeignKeysDescription foreign_keys;
    UniqueNotEnforcedDescription unique_not_enforced;

    if (create_query.columns_list)
    {
        if (create_query.columns_list->columns)
        {
            // Set attach = true to avoid making columns nullable due to ANSI settings, because the dialect change
            // should NOT affect existing tables.
            columns = InterpreterCreateQuery::getColumnsDescription(*create_query.columns_list->columns, context, /* attach= */ true);
        }

        if (create_query.columns_list->indices)
            for (const auto & index : create_query.columns_list->indices->children)
                indices.push_back(IndexDescription::getIndexFromAST(index->clone(), columns, context));

        if (create_query.columns_list->constraints)
            for (const auto & constraint : create_query.columns_list->constraints->children)
                constraints.constraints.push_back(std::dynamic_pointer_cast<ASTConstraintDeclaration>(constraint->clone()));

        if (create_query.columns_list->foreign_keys)
            for (const auto & foreign_key : create_query.columns_list->foreign_keys->children)
                foreign_keys.foreign_keys.push_back(std::dynamic_pointer_cast<ASTForeignKeyDeclaration>(foreign_key->clone()));

        if (create_query.columns_list->unique)
            for (const auto & unique : create_query.columns_list->unique->children)
                unique_not_enforced.unique.push_back(std::dynamic_pointer_cast<ASTUniqueNotEnforcedDeclaration>(unique->clone()));
    }
    else
        throw Exception("Incorrect CREATE query: required list of column descriptions or AS section or SELECT.", ErrorCodes::INCORRECT_QUERY);

    /// Even if query has list of columns, canonicalize it (unfold Nested columns).
    ASTPtr new_columns = InterpreterCreateQuery::formatColumns(columns, ParserSettings::valueOf(context->getSettingsRef()));
    ASTPtr new_indices = InterpreterCreateQuery::formatIndices(indices);
    ASTPtr new_constraints = InterpreterCreateQuery::formatConstraints(constraints);
    ASTPtr new_foreign_keys = InterpreterCreateQuery::formatForeignKeys(foreign_keys);
    ASTPtr new_unique_not_enforced = InterpreterCreateQuery::formatUnique(unique_not_enforced);

    if (create_query.columns_list->columns)
        create_query.columns_list->replace(create_query.columns_list->columns, new_columns);

    if (create_query.columns_list->indices)
        create_query.columns_list->replace(create_query.columns_list->indices, new_indices);

    if (create_query.columns_list->constraints)
        create_query.columns_list->replace(create_query.columns_list->constraints, new_constraints);

    if (create_query.columns_list->foreign_keys)
        create_query.columns_list->replace(create_query.columns_list->foreign_keys, new_foreign_keys);

    if (create_query.columns_list->unique)
        create_query.columns_list->replace(create_query.columns_list->unique, new_unique_not_enforced);

    /// Check for duplicates
    std::set<String> all_columns;
    for (const auto & column : columns)
    {
        if (!all_columns.emplace(column.name).second)
            throw Exception("Column " + backQuoteIfNeed(column.name) + " already exists", ErrorCodes::DUPLICATE_COLUMN);
    }

    /// Table constructing
    return StorageFactory::instance().get(create_query, "", context, context->getGlobalContext(), columns, constraints, foreign_keys, unique_not_enforced, false);
}

/// TODO: impl based on createStorageFromQuery(create_query, context) ?
StoragePtr createStorageFromQuery(const String & query, const ContextPtr & context)
{
    auto ast = getASTCreateQueryFromString(query, context);

    ColumnsDescription columns;
    if (ast->columns_list && ast->columns_list->columns)
    {
        columns = InterpreterCreateQuery::getColumnsDescription(*ast->columns_list->columns, context, true /*attach*/);
    }
    ConstraintsDescription constrants;
    if (ast->columns_list && ast->columns_list->constraints)
    {
        constrants = InterpreterCreateQuery::getConstraintsDescription(ast->columns_list->constraints);
    }
    ForeignKeysDescription foreign_keys;
    if (ast->columns_list && ast->columns_list->foreign_keys)
    {
        foreign_keys = InterpreterCreateQuery::getForeignKeysDescription(ast->columns_list->foreign_keys);
    }
    UniqueNotEnforcedDescription unique_not_enforced;
    if (ast->columns_list && ast->columns_list->unique)
    {
        unique_not_enforced = InterpreterCreateQuery::getUniqueNotEnforcedDescription(ast->columns_list->unique);
    }

    return StorageFactory::instance().get(
        *ast,
        "",
        Context::createCopy(context),
        context->getGlobalContext(),
        // Set attach = true to avoid making columns nullable due to ANSI settings, because the dialect change
        // should NOT affect existing tables.
        columns,
        constrants,
        foreign_keys,
        unique_not_enforced,
        false /*has_force_restore_data_flag*/);
}

void replaceCnchWithCloud(
    ASTStorage * storage,
    const String & cnch_database,
    const String & cnch_table,
    WorkerEngineType engine_type,
    const Strings & engine_args)
{
    if (!startsWith(storage->engine->name, "Cnch"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expect Cnch-family Engine but got {}", storage->engine->name);

    auto engine = std::make_shared<ASTFunction>();
    engine->name = storage->engine->name.replace(0, strlen("Cnch"), toString(engine_type));
    engine->arguments = std::make_shared<ASTExpressionList>();
    engine->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(cnch_database));
    engine->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(cnch_table));
    if (!engine_args.empty())
    {
        for (const auto & arg : engine_args)
        {
            engine->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(arg));
        }
    }
    else if (storage->engine->arguments)
    {
        for (const auto & arg : storage->engine->arguments->children)
        {
            engine->arguments->children.push_back(arg);
        }
    }
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
