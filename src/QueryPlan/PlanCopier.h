#pragma once

#include <Core/Names.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/SymbolAllocator.h>
#include <QueryPlan/ProjectionStep.h>

#include <memory>

namespace DB
{

// todo@kaixi: implement for all plan nodes
class PlanCopier
{
public:
    static std::shared_ptr<ProjectionStep> reallocateWithProjection(
        const DataStream & data_stream, SymbolAllocator & symbolAllocator,
        std::unordered_map<std::string, std::string> & reallocated_names);

    static PlanNodePtr copy(const PlanNodePtr & plan, ContextMutablePtr & context);

    static bool isOverlapping(const DataStream & data_stream, const DataStream & data_stream2);

    PlanCopier() = delete;
};

}

