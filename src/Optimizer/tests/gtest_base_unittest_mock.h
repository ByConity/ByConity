#pragma once

#include <cstddef>
#include <memory>
#include <Core/NameToType.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanNodeIdAllocator.h>
#include "Optimizer/PredicateConst.h"
#include "Parsers/ASTTablesInSelectQuery.h"
#include "Parsers/IAST_fwd.h"
#include "QueryPlan/QueryPlan.h"

namespace DB
{
class MockedPlanNode;
using MockedPlanNodePtr = std::shared_ptr<MockedPlanNode>;
using MockedPlanNodes = std::vector<MockedPlanNodePtr>;

struct HandlerContext
{
    PlanNodes inputs;
    ContextMutablePtr context;
    CTEId cte_id;
};

class MockedPlanNode
{
public:
    static MockedPlanNodePtr of(std::function<QueryPlanStepPtr(const HandlerContext &)> handler_)
    {
        return std::make_shared<MockedPlanNode>(std::move(handler_));
    }

    void setCTEDef(PlanNodePtr cte_def_) { cte_def = std::move(cte_def_); }

    explicit MockedPlanNode(std::function<QueryPlanStepPtr(const HandlerContext &)> handler_) : handler(std::move(handler_)) { }

private:
    static PlanNodePtr recursiveBuildSelf(MockedPlanNodePtr root, const ContextMutablePtr & context, CTEInfo & cte_info);

    friend class PlanMocker;
    std::function<QueryPlanStepPtr(const HandlerContext &)> handler;
    MockedPlanNodes children;
    PlanNodePtr cte_def; // only for building CTERefStep
};

class PlanMocker
{
public:
    explicit PlanMocker(ContextMutablePtr context_) : context(std::move(context_)) { }
    PlanMocker & add(const MockedPlanNodePtr & child);

    PlanMocker & addChildren() { return *this; }

    template <typename T, typename... Args>
    PlanMocker & addChildren(T child, const Args &... args)
    {
        if (!current_node)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Optimizer MockedPlanNode Error, use addChildren() after add(join/union)!");

        {
            current_node->children.push_back(child.root);
        }
        return addChildren(args...);
    }

    PlanNodePtr build();
    PlanNodePtr buildWithCTE(CTEInfo & cte_info);

private:
    MockedPlanNodePtr root;
    MockedPlanNodePtr current_node;

    ContextMutablePtr context;
};

namespace CreateMockedPlanNode
{
    MockedPlanNodePtr values(const Names & columns = {}, Fields values = {});
    MockedPlanNodePtr values(const Block & header);
    // MockedPlanNodePtr tableScan(String database, String table, Names columns);

    MockedPlanNodePtr limit(size_t limit, size_t offset = 0, bool partial = false);

    MockedPlanNodePtr projection(const Assignments & assignments, const NameToType & name_to_type, bool final_project = false);
    MockedPlanNodePtr projection(const NameSet & allow_names = {}, bool final_project = false);

    MockedPlanNodePtr filter(ConstASTPtr filter);
    MockedPlanNodePtr sorting(SortDescription description, size_t limit = 0, SortingStep::Stage stage = SortingStep::Stage::FULL);
    MockedPlanNodePtr exchange(const ExchangeMode & mode, Partitioning schema, bool keep_order = false);

    MockedPlanNodePtr join(
        Names left_keys,
        Names right_keys,
        ConstASTPtr join_filter = PredicateConst::TRUE_VALUE,
        ASTTableJoin::Kind kind = ASTTableJoin::Kind::Inner,
        ASTTableJoin::Strictness strictness = ASTTableJoin::Strictness::All,
        std::vector<bool> key_ids_null_safe = {});
    
    MockedPlanNodePtr unionn(DataStream output_stream = {}, OutputToInputs output_to_inputs = {});

    MockedPlanNodePtr distinct(const Names & columns);
    MockedPlanNodePtr distinct(const SizeLimits & set_size_limits, UInt64 limit_hint, const Names & columns, bool pre_distinct);

    MockedPlanNodePtr aggregating(const Names & keys, AggregateDescriptions aggregates = {}, bool final = true);

    MockedPlanNodePtr cte(PlanNodePtr cte_def, std::unordered_map<String, String> output_columns = {}, bool has_filter = false);
}


}
