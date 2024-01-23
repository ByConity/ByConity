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

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
    /// Collect columns and values for WHERE condition with AND and EQUALS (case insesitive)
    /// e.g for query "select ... where db = 'x' ", method will return {'db' : 'x'} map
    void collectWhereClausePredicate(const ASTPtr & ast, std::map<String,String> & columnToValue);
    std::vector<std::map<String,Field>> collectWhereORClausePredicate(const ASTPtr & ast, const ContextPtr & context, bool or_ret_empty = false);
    bool extractNameFromWhereClause(const ASTPtr & node, const String & key_name, String & ret);
}
