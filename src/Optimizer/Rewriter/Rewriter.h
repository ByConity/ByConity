#pragma once

#include <Interpreters/Context.h>
#include <QueryPlan/PlanNode.h>

namespace DB
{
class Rewriter;

using RewriterPtr = std::shared_ptr<Rewriter>;
using Rewriters = std::vector<RewriterPtr>;

class Rewriter
{
public:
    virtual ~Rewriter() = default;
    virtual void rewrite(QueryPlan & plan, ContextMutablePtr context) const = 0;
    virtual String name() const = 0;
};
}
