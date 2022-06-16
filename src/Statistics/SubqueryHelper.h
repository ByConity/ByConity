#pragma once
#include <memory>
#include <Interpreters/Context.h>

namespace DB::Statistics
{
class SubqueryHelper
{
public:
    static SubqueryHelper create(ContextPtr context, const String & sql);
    SubqueryHelper(SubqueryHelper && other) = default;
    Block getNextBlock();
    ~SubqueryHelper();

    struct DataImpl;

private:
    explicit SubqueryHelper(std::unique_ptr<DataImpl> impl_);

    std::unique_ptr<DataImpl> impl;
    // use unique_ptr to ensure that sub_context will be destroyed after block_io
};

// if don't care about result, use this
void executeSubQuery(ContextPtr context, const String & sql);

}
