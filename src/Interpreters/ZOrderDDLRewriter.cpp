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

#include <Interpreters/ZOrderDDLRewriter.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

void ZOrderDDLRewriter::apply(IAST * ast)
{
    if (auto * create = ast->as<ASTCreateQuery>())
    {
        auto columns = getColumnsInOrderByWithSpaceFillingCurves(create);
        if (columns.size() <= 1)
            return;
        MergeTreeSettings default_settings;
        size_t z_index_granularity = default_settings.z_index_granularity.value;
        bool allow_rewrite = default_settings.enable_index_by_space_filling_curve;
        auto * settings = create->storage->settings;
        if (settings)
        {
            auto * allow_rewrite_change = settings->changes.tryGet("enable_index_by_space_filling_curve");
            if (allow_rewrite_change)
                allow_rewrite = allow_rewrite_change->safeGet<UInt64>();
            if (!allow_rewrite)
                return;
            auto * z_index_granularity_change = settings->changes.tryGet("z_index_granularity");
            if (z_index_granularity_change)
                z_index_granularity = z_index_granularity_change->safeGet<UInt64>();
        }

        /// Create minmax index for all columns
        if (!create->columns_list->indices)
        {
            auto indices = std::make_shared<ASTExpressionList>();
            create->columns_list->children.push_back(indices);
            create->columns_list->indices = indices.get();
        }

        for (const auto & column : columns)
        {
            auto indice = std::make_shared<ASTIndexDeclaration>();
            indice->name = fmt::format("{}_{}", magic_index_prefix, column);
            indice->granularity = z_index_granularity;
            auto expr = std::make_shared<ASTIdentifier>(column);
            indice->children.push_back(expr);
            indice->expr = expr.get();
            auto type = std::make_shared<ASTFunction>();
            type->name = "minmax";
            indice->children.push_back(type);
            indice->type = type.get();
            create->columns_list->indices->children.push_back(std::move(indice));
        }
        /// Remove primary key
        if (create->storage->primary_key)
        {
            auto & children = create->storage->children;
            std::erase_if(children, [&](const auto & child) { return child.get() == create->storage->primary_key; });
            create->storage->primary_key = nullptr;
        }
        auto primary_key = std::make_shared<ASTFunction>();
        primary_key->name = "tuple";
        create->storage->children.push_back(primary_key);
        create->storage->primary_key = primary_key.get();
    }
}

Names ZOrderDDLRewriter::getColumnsInOrderByWithSpaceFillingCurves(const ASTCreateQuery * create)
{
    Names columns;
    const auto * storage = create->storage;
    if (!storage || !storage->order_by)
        return {};

    const auto * f = storage->order_by->as<const ASTFunction>();
    if (!f || !space_filling_curves.contains(Poco::toLower(f->name)))
        return {};

    for (const auto & child : f->arguments->children)
        if (const auto * identifier = child->as<const ASTIdentifier>())
            columns.push_back(identifier->name());
    return columns;
}

}
