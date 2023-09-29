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

#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTQueryWithOutput.h>

#include <unordered_map>

namespace DB
{

class ASTReproduceQuery : public ASTQueryWithOutput
{
public:
    enum class Mode
    {
        DDL,
        EXPLAIN,
        EXECUTE
    };

    enum class Expression: uint8_t
    {
        SUBQUERY,
        QUERY_ID,
        REPRODUCE_PATH,
        SETTINGS_CHANGES,
        CLUSTER,
    };

    String getID(char) const override { return "ReproduceQuery"; }

    ASTType getType() const override { return ASTType::ASTReproduceQuery; }

    ASTPtr clone() const override;

    void setExpression(Expression expr, ASTPtr && ast);

    ASTPtr subquery() const { return getExpression(Expression::SUBQUERY); }
    ASTPtr queryId() const { return getExpression(Expression::QUERY_ID); }
    ASTPtr reproducePath() const { return getExpression(Expression::REPRODUCE_PATH); }
    ASTPtr settingsChanges() const { return getExpression(Expression::SETTINGS_CHANGES); }
    ASTPtr cluster() const { return getExpression(Expression::CLUSTER); }

    ASTPtr getExpression(Expression expr, bool clone = false) const
    {
        auto it = positions.find(expr);
        if (it != positions.end())
            return clone ? children[it->second]->clone() : children[it->second];
        return {};
    }

    Mode mode;
    bool is_verbose;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    std::unordered_map<Expression, size_t> positions;
    ASTPtr & getExpression(Expression expr);
};
}
