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

#include <Common/Logger.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/profile/ProfileLogHub.h>

namespace DB
{

/// Returns single row with explain results
class InterpreterExplainQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterExplainQuery(const ASTPtr & query_, ContextMutablePtr context_) : WithMutableContext(context_),
        query(query_), log(getLogger("InterpreterExplainQuery")) {}

    BlockIO execute() override;

    Block getSampleBlock();

    static void fillColumn(IColumn & column, const std::string & str);

private:
    ASTPtr query;
    LoggerPtr log;
    SelectQueryOptions options;

    BlockInputStreamPtr executeImpl();

    void rewriteDistributedToLocal(ASTPtr & ast);

    void elementDatabaseAndTable(const ASTSelectQuery & select_query, const ASTPtr & where, WriteBuffer & buffer);

    void elementWhere(const ASTPtr & where, WriteBuffer & buffer);

    void elementDimensions(const ASTPtr & select, WriteBuffer & buffer);

    void elementMetrics(const ASTPtr & select, WriteBuffer & buffer);

    void elementGroupBy(const ASTPtr & group_by, WriteBuffer & buffer);

    void listPartitionKeys(StoragePtr & storage, WriteBuffer & buffer);

    void listRowsOfOnePartition(StoragePtr & storage, const ASTPtr & group_by, const ASTPtr & where, WriteBuffer & buffer);

    std::optional<String> getActivePartCondition(StoragePtr & storage);

    BlockInputStreamPtr explain();

    BlockInputStreamPtr explainUsingOptimizer();

    BlockInputStreamPtr explainMetaData();

    void explainPlanWithOptimizer(const ASTExplainQuery & explain_ast, QueryPlan & plan, WriteBuffer & buffer, ContextMutablePtr & context_ptr, bool & single_line);

    void explainDistributedWithOptimizer(const ASTExplainQuery & explain_ast, QueryPlan & plan, WriteBuffer & buffer, ContextMutablePtr & context_ptr);

    BlockIO explainAnalyze();

    void explainPipelineWithOptimizer(const ASTExplainQuery & explain_ast, QueryPlan & plan, WriteBuffer & buffer, ContextMutablePtr & context_ptr);
};


class ExplainConsumer: public ProfileElementConsumer<ProcessorProfileLogElement>
{
public:
    explicit ExplainConsumer(std::string query_id): ProfileElementConsumer(query_id) {}
    void consume(ProcessorProfileLogElement & element) override;

    std::vector<ProcessorProfileLogElement> getStoreResult() const {return store_vector;}
    std::vector<ProcessorProfileLogElement> store_vector;
};

}
