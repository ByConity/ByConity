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

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Statistics/CollectorSettings.h>
#include <Statistics/StatisticsCommon.h>

namespace DB
{
class Context;


class InterpreterShowStatsQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowStatsQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;


private:
    BlockIO executeTable();
    BlockIO executeColumn();
    void executeSpecial();

    ASTPtr query_ptr;
    // currently only cache_policy is useful
    Statistics::CollectorSettings collector_settings;
};

}
