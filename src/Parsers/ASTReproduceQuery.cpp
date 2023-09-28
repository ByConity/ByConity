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

#include <Parsers/ASTReproduceQuery.h>

#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTQueryWithOutput.h>

#include <memory>
#include <unordered_map>

namespace DB
{

ASTPtr ASTReproduceQuery::clone() const
{
    auto res = std::make_shared<ASTReproduceQuery>(*this);
    res->children.clear();
    res->positions.clear();

#define CLONE(expr) res->setExpression(expr, getExpression(expr, true))

    CLONE(Expression::SUBQUERY);
    CLONE(Expression::QUERY_ID);
    CLONE(Expression::REPRODUCE_PATH);
    CLONE(Expression::SETTINGS_CHANGES);
    CLONE(Expression::CLUSTER);

#undef CLONE

    return res;
}

void ASTReproduceQuery::setExpression(Expression expr, ASTPtr && ast)
{
    if (ast)
    {
        auto it = positions.find(expr);
        if (it == positions.end())
        {
            positions[expr] = children.size();
            children.emplace_back(ast);
        }
        else
            children[it->second] = ast;
    }
    else if (positions.count(expr))
    {
        size_t pos = positions[expr];
        children.erase(children.begin() + pos);
        positions.erase(expr);
        for (auto & pr : positions)
            if (pr.second > pos)
                --pr.second;
    }
}

void ASTReproduceQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "REPRODUCE";

    switch (mode)
    {
        case ASTReproduceQuery::Mode::DDL:
            settings.ostr << " DDL" << (settings.hilite ? hilite_none : "");
            break;
        case ASTReproduceQuery::Mode::EXPLAIN:
            if (is_verbose)
                settings.ostr << " VERBOSE EXPLAIN" << (settings.hilite ? hilite_none : "");
            else
                settings.ostr << " EXPLAIN" << (settings.hilite ? hilite_none : "");
            break;
        case ASTReproduceQuery::Mode::EXECUTE:
            if (is_verbose)
                settings.ostr << " VERBOSE EXECUTE" << (settings.hilite ? hilite_none : "");
            else
                settings.ostr << " EXECUTE" << (settings.hilite ? hilite_none : "");
            break;
    }

    if (cluster())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
                      << " ON CLUSTER " << (settings.hilite ? hilite_none : "");
        cluster()->formatImpl(settings, state, frame);
    }


    if (subquery())
    {
        settings.ostr << settings.nl_or_ws
                      << (settings.hilite ? hilite_keyword : "")
                      << "QUERY " << (settings.hilite ? hilite_none : "")
                      << settings.nl_or_ws;
        ++frame.indent;
        subquery()->formatImpl(settings, state, frame);
        --frame.indent;
    }
    else if (queryId())
    {
        settings.ostr << settings.nl_or_ws
                      << (settings.hilite ? hilite_keyword : "")
                      << "ID " << (settings.hilite ? hilite_none : "");
        queryId()->formatImpl(settings, state, frame);
    }

    settings.ostr << settings.nl_or_ws
                  << (settings.hilite ? hilite_keyword : "")
                  << "SOURCE " << (settings.hilite ? hilite_none : "");
    reproducePath()->formatImpl(settings, state, frame);

    if (settingsChanges())
    {
        settings.ostr << settings.nl_or_ws
                      << (settings.hilite ? hilite_keyword : "")
                      << "SETTINGS " << (settings.hilite ? hilite_none : "");
        settingsChanges()->formatImpl(settings, state, frame);
    }
}

ASTPtr & ASTReproduceQuery::getExpression(Expression expr)
{
    if (!positions.count(expr))
        throw Exception("Get expression before set", ErrorCodes::LOGICAL_ERROR);
    return children[positions[expr]];
}
}
