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

#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class Context;

class QueryUseOptimizerChecker
{
public:
    static bool check(ASTPtr & node, const ContextMutablePtr & context, bool use_distributed_stages = false);
};

struct QueryUseOptimizerContext
{
    const ContextMutablePtr & context;
    NameSet & with_tables;
    Tables external_tables;
};

class QueryUseOptimizerVisitor : public ASTVisitor<bool, QueryUseOptimizerContext>
{
public:
    bool visitNode(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTSelectQuery(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTTableJoin(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTIdentifier(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTFunction(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTQuantifiedComparison(ASTPtr & node, QueryUseOptimizerContext &) override;
    const String & getReason() const { return reason; }

private:
    void collectWithTableNames(ASTSelectQuery & query, NameSet & with_tables);
    String reason;
};

class QuerySupportOptimizerVisitor : public ASTVisitor<bool, QueryUseOptimizerContext>
{
public:
    bool visitNode(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTSelectQuery(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTSelectIntersectExceptQuery(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTSelectWithUnionQuery(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTTableJoin(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTFunction(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTQuantifiedComparison(ASTPtr & node, QueryUseOptimizerContext &) override;

private:
};

}
