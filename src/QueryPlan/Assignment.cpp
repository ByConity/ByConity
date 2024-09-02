#include <QueryPlan/Assignment.h>

namespace DB
{
Assignments Assignments::copy() const
{
    Assignments copied;
    for (const auto & [symbol, ast] : *this)
        copied.emplace(symbol, ast->clone());
    return copied;
}
}
