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

#include <Functions/FunctionFactory.h>
#include <Optimizer/ExpressionRewriter.h>
#include <Parsers/ASTVisitor.h>

namespace DB
{
ASTPtr ExpressionRewriter::rewrite(const ConstASTPtr & expression, ConstASTMap & expression_map)
{
    ExpressionRewriterVisitor visitor;
    return ASTVisitorUtil::accept(expression->clone(), visitor, expression_map);
}

ASTPtr ExpressionRewriterVisitor::visitNode(ASTPtr & node, ConstASTMap & expression_map)
{
    auto result = SimpleExpressionRewriter::visitNode(node, expression_map);
    if (expression_map.contains(result))
        return expression_map[result]->clone();
    return result;
}

bool FunctionIsInjective::isInjective(const ConstASTPtr & expr, ContextMutablePtr & context, const NamesAndTypes & input_types, const NameSet & partition_cols)
{
    Analysis analysis;
    FieldDescriptions fields;
    for (const auto & input : input_types)
    {
        FieldDescription field{input.name, input.type};
        fields.emplace_back(field);
    }
    Scope scope(Scope::ScopeType::RELATION, nullptr, true, fields);
    ExprAnalyzerOptions options;
    ASTPtr tmp_expr = std::const_pointer_cast<IAST>(expr);
    ExprAnalyzer::analyze(tmp_expr, &scope, context, analysis, options);
    FunctionIsInjectiveVisitor visitor{context, analysis.getExpressionColumnWithTypes()};
    NameSet remind_partition_cols = partition_cols;
    return ASTVisitorUtil::accept(expr, visitor, remind_partition_cols) && remind_partition_cols.empty();
}

bool FunctionIsInjectiveVisitor::visitNode(const ConstASTPtr & node, NameSet & c)
{
    bool is_injective = true;
    for (ConstASTPtr child : node->children)
    {
        is_injective &= ASTVisitorUtil::accept(child, *this, c);
    }
    return is_injective;
}

bool FunctionIsInjectiveVisitor::visitASTIdentifier(const ConstASTPtr & node, NameSet & c)
{
    auto identifier = node->as<ASTIdentifier>();
    c.erase(identifier->name());
    return true;
}

bool FunctionIsInjectiveVisitor::visitASTFunction(const ConstASTPtr & node, NameSet & c)
{
    auto function = node->as<ASTFunction>();
    FunctionOverloadResolverPtr function_builder = FunctionFactory::instance().get(function->name, context);
    ColumnsWithTypeAndName processed_arguments;
    for (auto & arg : function->arguments->children)
    {
        auto & col_type = expr_types[arg];
        processed_arguments.emplace_back(col_type.column, col_type.type, arg->getColumnName());
    }
    auto function_base = function_builder->build(processed_arguments);
    bool is_injective = function_base->isInjective(processed_arguments);
    if (is_injective)
    {
        visitNode(node, c);
    }
    return is_injective;
}
}
