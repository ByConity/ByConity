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

#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{

struct ExecutePrewhereSubquery
{
    using Data = ExecutePrewhereSubquery;

    ContextMutablePtr context;
    explicit ExecutePrewhereSubquery(ContextMutablePtr context_) : context(std::move(context_)) {}

    static void visit(ASTPtr & ast, Data & impl)
    {
        if (auto * subquery = typeid_cast<ASTSubquery *>(ast.get()))
            impl.visit(*subquery, ast);
        else if (auto * function = typeid_cast<ASTFunction *>(ast.get()))
            impl.visit(*function, ast);
    }

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

    void visit(ASTSubquery & subquery, ASTPtr & ast) const;
    void visit(ASTFunction & function, ASTPtr & ast) const;

    void rewriteSubqueryToScalarLiteral(ASTSubquery & subquery, ASTPtr & ast) const;
    bool rewriteSubqueryToSet(ASTSubquery & subquery, ASTPtr & ast) const;
};

using ExecutePrewhereSubqueryVisitor = InDepthNodeVisitor<ExecutePrewhereSubquery, true>;

}
