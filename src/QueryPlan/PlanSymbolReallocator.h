#pragma once

#include <Core/Names.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/ExceptStep.h>
#include <QueryPlan/MarkDistinctStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SymbolAllocator.h>
#include <QueryPlan/SymbolMapper.h>

#include <memory>
#include <unordered_map>

namespace DB
{
class PlanSymbolReallocator
{
public:
    /* unalias symbol references that are just aliases of each other. */
    static PlanNodePtr unalias(const PlanNodePtr & plan, ContextMutablePtr & context);
    
    /* 
     * deep copy a plan with symbol reallocated. 
     * symbol_mapping return all symbol reallocated mapping.
     */
    static PlanNodePtr
    reallocate(const PlanNodePtr & plan, ContextMutablePtr & context, std::unordered_map<std::string, std::string> & symbol_mapping);

    /* check output stream is overlapping */
    static bool isOverlapping(const DataStream & lho, const DataStream & rho);

private:
    class SymbolReallocatorVisitor;
};

}
