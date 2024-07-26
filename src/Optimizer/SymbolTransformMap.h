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

#pragma once

#include <Optimizer/SimpleExpressionRewriter.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/PlanNode.h>

#include <optional>
#include <unordered_map>

namespace DB
{
/**
 * Used to determines the origin of identifier in expression.
 */
class SymbolTransformMap
{
public:
    static std::optional<SymbolTransformMap> buildFrom(PlanNodeBase & plan, std::optional<PlanNodeId> stop_node = std::nullopt);

    ASTPtr inlineReferences(const ConstASTPtr & expression) const;

    ASTPtr inlineReferences(const String & symbol) const { return inlineReferences(std::make_shared<ASTIdentifier>(symbol)); }

    String toString() const;

private:
    /**
     * violation may happen when illegal plan has symbol transform loop, or contain duplicate
     * symbol names with other plan nodes. eg, expr1 := cast(expr1, 'UInt8').
     */
    bool addSymbolMapping(const String & symbol, ConstASTPtr expr);

    std::unordered_map<String, ConstASTPtr> symbol_to_expressions;

    // cache
    mutable std::unordered_map<String, ConstASTPtr> expression_lineage;

    class Visitor;
    class Rewriter;
};

class SymbolTranslationMap
{
public:
    void addTranslation(ASTPtr ast, String name)
    {
        translation.emplace(std::move(ast), std::move(name));
    }
    // rewrite table column to ASTColumnReference before adding translation
    void addStorageTranslation(ASTPtr ast, String name, const IStorage * storage, UInt32 unique_id);
    std::optional<String> tryGetTranslation(const ASTPtr & expr) const;
    ASTPtr translate(ASTPtr ast) const
    {
        return translateImpl(ast);
    }

private:
    ASTMap<String> translation;

    ASTPtr translateImpl(ASTPtr ast) const;
};

class IdentifierToColumnReference : public SimpleExpressionRewriter<Void>
{
public:
    static ASTPtr rewrite(const IStorage * storage, UInt32 unique_id, ASTPtr ast, bool clone = true);

private:
    const IStorage * storage;
    UInt32 unique_id;
    StorageMetadataPtr storage_metadata;

public:
    IdentifierToColumnReference(const IStorage * storage_, UInt32 unique_id_);
    ASTPtr visitASTIdentifier(ASTPtr & node, Void & context) override;
};

class ColumnReferenceToIdentifier : public SimpleExpressionRewriter<Void>
{
public:
    static ASTPtr rewrite(ASTPtr ast, bool clone = true);
    ASTPtr visitASTTableColumnReference(ASTPtr & node, Void & context) override;
};
}
