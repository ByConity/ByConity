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

#include <Analyzers/ASTEquals.h>
#include <Analyzers/TypeAnalyzer.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/EqualityASTMap.h>
#include <Optimizer/SimpleExpressionRewriter.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>

#include <unordered_map>

namespace DB
{
using ConstASTMap = EqualityASTMap<ConstHashAST>;

class ExpressionRewriter
{
public:
    static ASTPtr rewrite(const ConstASTPtr & expression, ConstASTMap & expression_map);
};

class ExpressionRewriterVisitor : public SimpleExpressionRewriter<ConstASTMap>
{
public:
    ASTPtr visitNode(ASTPtr & node, ConstASTMap & expression_map) override;
};

class FunctionIsInjective
{
public:
    static bool isInjective(const ConstASTPtr & expr, ContextMutablePtr & context, const NamesAndTypes & input_types, const NameSet & partition_cols);
};

class FunctionIsInjectiveVisitor : public ConstASTVisitor<bool, NameSet>
{
public:
    FunctionIsInjectiveVisitor(ContextMutablePtr & context_, const std::unordered_map<ASTPtr, ColumnWithType> & expr_types_)
        : context(context_), expr_types(expr_types_)
    {
    }
    bool visitNode(const ConstASTPtr &, NameSet & context) override;
    bool visitASTFunction(const ConstASTPtr &, NameSet & context) override;
    bool visitASTIdentifier(const ConstASTPtr &, NameSet & context) override;

private:
    ContextMutablePtr & context;
    std::unordered_map<ASTPtr, ColumnWithType> expr_types;
};
}
