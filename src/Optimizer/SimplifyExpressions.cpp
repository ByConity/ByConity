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

#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SimplifyExpressions.h>
#include <Parsers/formatAST.h>
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTIdentifier.h"

namespace DB
{
ConstASTPtr CommonPredicatesRewriter::rewrite(const ConstASTPtr & predicate, ContextMutablePtr & context)
{
    CommonPredicatesRewriter rewriter;
    NodeContext node_context{.root = NodeContext::Root::ROOT_NODE, .context = context};
    return ASTVisitorUtil::accept(predicate, rewriter, node_context);
}

ConstASTPtr CommonPredicatesRewriter::visitNode(const ConstASTPtr & node, NodeContext & context)
{
    ASTs children;
    for (ConstASTPtr child : node->children)
    {
        ASTPtr ast = ASTVisitorUtil::accept(child, *this, context)->clone();
        children.emplace_back(ast);
    }
    auto new_node = node->clone();
    new_node->replaceChildren(children);
    return new_node;
}

ConstASTPtr CommonPredicatesRewriter::visitASTFunction(const ConstASTPtr & node, NodeContext & node_context)
{
    const auto & fun = node->as<ASTFunction &>();
    if (fun.name == PredicateConst::AND || fun.name == PredicateConst::OR)
    {
        std::vector<ConstASTPtr> extracted_predicates = PredicateUtils::extractPredicate(node);
        std::vector<ConstASTPtr> result;
        for (auto & predicate : extracted_predicates)
        {
            NodeContext child_context{.root = NodeContext::Root::NOT_ROOT_NODE, .context = node_context.context};
            result.emplace_back(process(predicate, child_context));
        }
        ASTPtr combined_predicate = PredicateUtils::combinePredicates(fun.name, result);
        const auto & combined_fun = combined_predicate->as<ASTFunction>();
        if (combined_fun == nullptr || (combined_fun->name != PredicateConst::AND && combined_fun->name != PredicateConst::OR))
        {
            return combined_predicate;
        }
        auto simplified = PredicateUtils::extractCommonPredicates(combined_predicate, node_context.context);
        // Prefer AND at the root if possible
        const auto & simplified_fun = simplified->as<ASTFunction>();
        if (node_context.root == NodeContext::Root::ROOT_NODE && simplified_fun && simplified_fun->name == PredicateConst::OR)
        {
            return PredicateUtils::distributePredicate(simplified, node_context.context);
        }
        return simplified;
    }
    return node;
}

ConstASTPtr CommonPredicatesRewriter::process(const ConstASTPtr & node, NodeContext & context)
{
    return ASTVisitorUtil::accept(node, *this, context);
}

ConstASTPtr SwapPredicateRewriter::rewrite(const ConstASTPtr & predicate, ContextMutablePtr &)
{
    SwapPredicateRewriter visitor;
    Void visitor_context{};
    return ASTVisitorUtil::accept(predicate, visitor, visitor_context);
}

ConstASTPtr SwapPredicateRewriter::visitNode(const ConstASTPtr & node, Void & context)
{
    ASTs children;
    for (ConstASTPtr child : node->children)
    {
        ASTPtr ast = ASTVisitorUtil::accept(child, *this, context)->clone();
        children.emplace_back(ast);
    }
    auto new_node = node->clone();
    new_node->replaceChildren(children);
    return new_node;
}

ConstASTPtr SwapPredicateRewriter::visitASTFunction(const ConstASTPtr & predicate, Void & context)
{
    const auto & function = predicate->as<ASTFunction &>();
    if (function.name == "and")
    {
        std::vector<ConstASTPtr> conjuncts = PredicateUtils::extractConjuncts(predicate);
        ASTs reordered_conjunct;
        for (ConstASTPtr conjunct : conjuncts)
        {
            ASTPtr ast = ASTVisitorUtil::accept(conjunct, *this, context)->clone();
            reordered_conjunct.emplace_back(ast);
        }
        return makeASTFunction("and", reordered_conjunct);
    }
    if (function.name == "or")
    {
        std::vector<ConstASTPtr> disjuncts = PredicateUtils::extractDisjuncts(predicate);
        ASTs reordered_disjuncts;
        for (ConstASTPtr disjunct : disjuncts)
        {
            ASTPtr ast = ASTVisitorUtil::accept(disjunct, *this, context)->clone();
            reordered_disjuncts.emplace_back(ast);
        }
        return makeASTFunction("or", reordered_disjuncts);
    }
    if (function.name == "not")
    {
        ConstASTPtr sub = function.arguments->getChildren()[0];
        ASTPtr reorder_sub = ASTVisitorUtil::accept(sub, *this, context)->clone();
        return makeASTFunction("not", reorder_sub);
    }
    if (function.name == "equals")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("equals", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    if (function.name == "notEquals")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("notEquals", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    if (function.name == "greater")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("less", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    if (function.name == "greaterOrEquals")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("lessOrEquals", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    if (function.name == "less")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("greater", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    if (function.name == "lessOrEquals")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("greaterOrEquals", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    return predicate;
}

ConstASTPtr RemoveRedundantCastRewriter::rewrite(const ConstASTPtr & predicate, NameToType & column_types)
{
    RemoveRedundantCastRewriter rewriter;
    return ASTVisitorUtil::accept(predicate, rewriter, column_types);
}

ConstASTPtr RemoveRedundantCastRewriter::visitNode(const ConstASTPtr & node, NameToType & column_types)
{
    ASTs children;
    for (ConstASTPtr child : node->children)
    {
        ASTPtr ast = ASTVisitorUtil::accept(child, *this, column_types)->clone();
        children.emplace_back(ast);
    }
    auto new_node = node->clone();
    new_node->replaceChildren(children);

    return new_node;
}

ConstASTPtr RemoveRedundantCastRewriter::visitASTFunction(const ConstASTPtr & node, NameToType & column_types)
{
    auto * func = node->as<ASTFunction>();

    if (Poco::toLower(func->name) == "cast")
    {
        auto * cast_source = func->arguments->as<ASTExpressionList &>().children[0]->as<ASTIdentifier>();
        auto * target_name = func->arguments->getChildren()[1]->as<ASTLiteral>();
        if (cast_source && target_name)
        {
            auto target_type = DataTypeFactory::instance().get(target_name->value.safeGet<String>());
            if (column_types.contains(cast_source->name()))
            {
                const auto & source_type = column_types.at(cast_source->name());
                if (source_type->equals(*target_type))
                {
                    return std::make_shared<ASTIdentifier>(*cast_source);
                }
                // convert CAST(xxx, 'Nullable(String)') to xxx, where type of xxx is LowCardinality(Nullable(String)).
                else if (source_type->isLowCardinalityNullable() && target_type->isNullable())
                {
                    const auto & dict_type = std::dynamic_pointer_cast<const DataTypeLowCardinality>(source_type)->getDictionaryType();

                    if (dict_type->equals(*target_type))
                    {
                        return std::make_shared<ASTIdentifier>(*cast_source);
                    }
                }
            }
        }
    }
    else
    {
        ASTs children;
        // NOTE: use func->arguments->children instead of func->children, because func->children[0] is an ASTExpressionList, which children is real arguments.
        for (ConstASTPtr child : func->arguments->getChildren())
        {
            ASTPtr ast = ASTVisitorUtil::accept(child, *this, column_types)->clone();
            children.emplace_back(ast);
        }
    
        auto new_func = std::make_shared<ASTFunction>(*func);
        new_func->children.clear();
        new_func->arguments = std::make_shared<ASTExpressionList>();
        new_func->arguments->children = std::move(children);
        new_func->children.push_back(new_func->arguments);
        return new_func;
    }

    return node;
}

}
