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

#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/QueryPlan.h>
namespace DB
{
class Rewriter;

using RewriterPtr = std::shared_ptr<Rewriter>;
using Rewriters = std::vector<RewriterPtr>;
class Rewriter : public std::enable_shared_from_this<Rewriter>
{
public:
    virtual ~Rewriter() = default;
    virtual String name() const = 0;

    void rewritePlan(QueryPlan & plan, ContextMutablePtr context) const;

    RewriterPtr afterRules(Rewriters rewriters)
    {
        after_rules.insert(after_rules.end(), rewriters.begin(), rewriters.end());
        return shared_from_this();
    }

private:
    virtual bool rewrite(QueryPlan & plan, ContextMutablePtr context) const = 0;
    // every rewriter must set a setting to enable/disable it.
    virtual bool isEnabled(ContextMutablePtr context) const = 0;

    Rewriters after_rules;
};
}
