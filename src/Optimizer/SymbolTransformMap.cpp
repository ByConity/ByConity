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

#include <Optimizer/SymbolTransformMap.h>

#include <Interpreters/InDepthNodeVisitor.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTTableColumnReference.h>
#include <Parsers/formatAST.h>
#include <QueryPlan/PlanVisitor.h>

namespace DB
{
class SymbolTransformMap::Visitor : public PlanNodeVisitor<bool, Void>
{
public:
    explicit Visitor(std::optional<PlanNodeId> stop_node_) : stop_node(std::move(stop_node_))
    {
    }

    bool visitAggregatingNode(AggregatingNode & node, Void & context) override
    {
        const auto * agg_step = dynamic_cast<const AggregatingStep *>(node.getStep().get());
        for (const auto & aggregate_description : agg_step->getAggregates())
        {
            auto function = Utils::extractAggregateToFunction(aggregate_description);
            if (!symbol_transform_map.addSymbolMapping(aggregate_description.column_name, function))
                return false;
        }
        return visitChildren(node, context);
    }

    bool visitFilterNode(FilterNode & node, Void & context) override { return visitChildren(node, context); }

    bool visitProjectionNode(ProjectionNode & node, Void & context) override
    {
        const auto * project_step = dynamic_cast<const ProjectionStep *>(node.getStep().get());
        
        if (project_step->isFinalProject())
            return false;

        for (const auto & assignment : project_step->getAssignments())
        {
            if (Utils::isIdentity(assignment))
                continue;
            if (!symbol_transform_map.addSymbolMapping(assignment.first, assignment.second))
                return false;
            // if (const auto * function = dynamic_cast<const ASTFunction *>(assignment.second.get()))
            // {
            //     if (function->name == "cast" && TypeCoercion::compatible)
            //     {
            //         symbol_to_cast_lossless_expressions.emplace(assignment.first, function->children[0]);
            //     }
            // }
        }
        return visitChildren(node, context);
    }

    bool visitSortingNode(SortingNode & node, Void & context) override
    {
        return visitChildren(node, context);
    }

    bool visitJoinNode(JoinNode & node, Void & context) override { return visitChildren(node, context); }
    bool visitExchangeNode(ExchangeNode & node, Void & context) override { return visitChildren(node, context); }

    bool visitTableScanNode(TableScanNode & node, Void &) override
    {
        const auto *table_step = dynamic_cast<const TableScanStep *>(node.getStep().get());
        for (const auto & item : table_step->getColumnAlias())
        {
            auto column_reference = std::make_shared<ASTTableColumnReference>(table_step->getStorage().get(), node.getId(), item.first);
            if (!symbol_transform_map.addSymbolMapping(item.second, column_reference))
                return false;
        }

        for (const auto & item : table_step->getInlineExpressions())
        {
            auto inline_expr
                = IdentifierToColumnReference::rewrite(table_step->getStorage().get(), node.getId(), item.second->clone(), false);
            if (!symbol_transform_map.addSymbolMapping(item.first, inline_expr))
                return false;
        }
        return true;
    }

    bool visitPlanNode(PlanNodeBase &, Void &) override
    {
        return false;
    }

    bool visitChildren(PlanNodeBase & node, Void & context)
    {
        if (stop_node.has_value() && node.getId() == *stop_node)
            return true;
        
        for (auto & child : node.getChildren())
            if (!VisitorUtil::accept(*child, *this, context))
                return false;
        return true;
    }

    std::optional<PlanNodeId> stop_node; // visit this node, but not visit its descendant
    SymbolTransformMap symbol_transform_map;
};

class SymbolTransformMap::Rewriter : public SimpleExpressionRewriter<Void>
{
public:
    Rewriter(
        const std::unordered_map<String, ConstASTPtr> & symbol_to_expressions_,
        std::unordered_map<String, ConstASTPtr> & expression_lineage_)
        : symbol_to_expressions(symbol_to_expressions_)
        , expression_lineage(expression_lineage_)
    {
    }

    ASTPtr visitASTIdentifier(ASTPtr & expr, Void & context) override
    {
        const auto & name = expr->as<ASTIdentifier &>().name();

        if (auto iter = expression_lineage.find(name); iter != expression_lineage.end())
            return iter->second->clone();

        if (auto iter = symbol_to_expressions.find(name); iter != symbol_to_expressions.end())
        {
            ASTPtr rewrite = ASTVisitorUtil::accept(iter->second->clone(), *this, context);
            expression_lineage[name] = rewrite;
            return rewrite->clone();
        }

        return expr;
    }

private:
    const std::unordered_map<String, ConstASTPtr> & symbol_to_expressions;
    std::unordered_map<String, ConstASTPtr> & expression_lineage;
};

std::optional<SymbolTransformMap> SymbolTransformMap::buildFrom(PlanNodeBase & plan, std::optional<PlanNodeId> stop_node)
{
    Visitor visitor(stop_node);
    Void context;
    if (!VisitorUtil::accept(plan, visitor, context))
        return {};
    return std::move(visitor.symbol_transform_map);
}

ASTPtr SymbolTransformMap::inlineReferences(const ConstASTPtr & expression) const
{
    Rewriter rewriter{symbol_to_expressions, expression_lineage};
    Void context;
    return ASTVisitorUtil::accept(expression->clone(), rewriter, context);
}

String SymbolTransformMap::toString() const
{
    String str;
    str += "expression_lineage: ";
    for (const auto & x: expression_lineage)
        str += x.first + " = " + serializeAST(*x.second) + ", ";
    str += "symbol_to_expressions: ";
    for (const auto & x: symbol_to_expressions)
        str += x.first + " = " + serializeAST(*x.second) + ", ";
    return str;
}

bool SymbolTransformMap::addSymbolMapping(const String & symbol, ConstASTPtr expr)
{
    for (const auto & symbol_in_expr : SymbolsExtractor::extract(expr))
        if (symbol_to_expressions.contains(symbol_in_expr))
            return false;
    return symbol_to_expressions.emplace(symbol, std::move(expr)).second;
}

void SymbolTranslationMap::addStorageTranslation(ASTPtr ast, String name, const IStorage * storage, UInt32 unique_id)
{
    ast = IdentifierToColumnReference::rewrite(storage, unique_id, ast, true);
    translation.emplace(std::move(ast), std::move(name));
}

std::optional<String> SymbolTranslationMap::tryGetTranslation(const ASTPtr & expr) const
{
    std::optional<String> result = std::nullopt;

    if (auto iter = translation.find(expr); iter != translation.end())
        result = iter->second;

    return result;
}

ASTPtr SymbolTranslationMap::translateImpl(ASTPtr ast) const
{
    // expression which can be translated
    if (auto column = tryGetTranslation(ast))
        return std::make_shared<ASTIdentifier>(*column);

    // ASTFunction
    if (const auto * func = ast->as<ASTFunction>())
    {
        ASTs translated_arguments;

        if (func->arguments)
            for (const auto & arg : func->arguments->children)
                translated_arguments.push_back(translateImpl(arg));

        return makeASTFunction(func->name, translated_arguments);
    }

    // for the 4th argument of InternalRuntimeFilter
    if (const auto * expr_list = ast->as<ASTExpressionList>())
    {
        auto new_expr_list = std::make_shared<ASTExpressionList>();
        for (const auto & arg : expr_list->children)
            new_expr_list->children.push_back(translateImpl(arg));
        return new_expr_list;
    }

    // other ast type
    return ast;
}

ASTPtr IdentifierToColumnReference::rewrite(const IStorage * storage, UInt32 unique_id, ASTPtr ast, bool clone)
{
    if (clone)
        ast = ast->clone();
    IdentifierToColumnReference rewriter{storage, unique_id};
    Void context;
    return ASTVisitorUtil::accept(ast, rewriter, context);
}

IdentifierToColumnReference::IdentifierToColumnReference(const IStorage * storage_, UInt32 unique_id_)
    : storage(storage_), unique_id(unique_id_)
{
    if (storage == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "storage ptr is NULL");
    storage_metadata = storage->getInMemoryMetadataPtr();
}

ASTPtr IdentifierToColumnReference::visitASTIdentifier(ASTPtr & node, Void &)
{
    const auto & iden = node->as<ASTIdentifier &>();
    const auto & columns = storage_metadata->getColumns();
    if (columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, iden.name()))
        return std::make_shared<ASTTableColumnReference>(storage, unique_id, iden.name());
    return node;
}

ASTPtr ColumnReferenceToIdentifier::rewrite(ASTPtr ast, bool clone)
{
    if (clone)
        ast = ast->clone();
    ColumnReferenceToIdentifier rewriter;
    Void context;
    return ASTVisitorUtil::accept(ast, rewriter, context);
}

ASTPtr ColumnReferenceToIdentifier::visitASTTableColumnReference(ASTPtr & node, Void &)
{
    const auto & column_ref = node->as<ASTTableColumnReference &>();
    return std::make_shared<ASTIdentifier>(column_ref.column_name);
}
}
