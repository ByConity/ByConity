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

#include <memory>
#include <common/types.h>
#include <Core/Names.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
namespace DB
{


class IAST;

/** Rewrite the table create query if the ORDER BY clause following by a space-filling curve function.
 * For example:
 * Original DDL: CREATE TABLE wide_table (x UInt32, y UInt32, z UInt32) ENGINE = MergeTree ORDER BY mortonEncode(x, y, z) SETTINGS z_index_granularity = 2;
 * Rewritten DDL:
 * CREATE TABLE wide_table (
 *      x UInt32,
 *      y UInt32,
 *      z UInt32
 *      INDEX x_auto_minmax x TYPE minmax GRANULARITY 2,
 *      INDEX y_auto_minmax y TYPE minmax GRANULARITY 2,
 *      INDEX z_auto_minmax z TYPE minmax GRANULARITY 2
 * ) ENGINE = MergeTree
 * ORDER BY mortonEncode(x, y, z)
 * PRIMARY KEY tuple();
 *
 * Algorithm:
 * 1. Find the ORDER BY clause in the AST
 * 2. Find the function in the ORDER BY clause
 * 3. If there's no function or the function is not a space-filling curve function, return
 * 4. Collect the columns in the function and z-index granularity
 * 5. Generate the index DDL
 * 6. Inject the index DDL and `PRIMARY KEY tuple()` clause into the AST
 */

class OrderBySpaceFillingCurveDDLRewriter
{

static inline const NameSet space_filling_curves = {"mortonencode, zorder"};
static constexpr char const * magic_index_prefix = "autoZcreate";

public:
    static void apply(IAST * ast);

private:
    static Names getColumnsInOrderByWithSpaceFillingCurve(const ASTCreateQuery * create);
};

}
