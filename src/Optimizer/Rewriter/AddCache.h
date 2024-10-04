#pragma once

#include <Interpreters/Context_fwd.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/QueryPlan.h>

namespace DB
{

class AddCache : public Rewriter
{
public:
    String name() const override { return "AddCache"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_intermediate_result_cache; }
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
};

}
