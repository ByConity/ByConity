#include <algorithm>
#include <memory>
#include <variant>
#include <Optimizer/tests/gtest_base_unittest_mock.h>
#include "Core/ColumnsWithTypeAndName.h"
#include "DataTypes/DataTypeString.h"
#include "QueryPlan/AggregatingStep.h"
#include "QueryPlan/CTERefStep.h"
#include "QueryPlan/DistinctStep.h"
#include "QueryPlan/ExpandStep.h"
#include "QueryPlan/FilterStep.h"
#include "QueryPlan/IQueryPlanStep.h"
#include "QueryPlan/LimitStep.h"
#include "QueryPlan/PlanNode.h"
#include "QueryPlan/QueryPlan.h"
#include "QueryPlan/SortingStep.h"
#include "QueryPlan/UnionStep.h"
#include "QueryPlan/ValuesStep.h"

namespace DB
{

PlanMocker & PlanMocker::add(const MockedPlanNodePtr & child)
{
    if (!current_node)
    {
        root = child;
        current_node = root;
        return *this;
    }

    current_node->children.emplace_back(child);
    current_node = child;
    return *this;
}

PlanNodePtr PlanMocker::build()
{
    if (!root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Optimizer MockedPlanNode Error, empty plan!");
    CTEInfo cte_info;
    auto node = MockedPlanNode::recursiveBuildSelf(root, context, cte_info);
    if (!cte_info.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Optimizer MockedPlanNode Error, use buildAndGetCTEInfo() for CTERefStep!");
    return node;
}

PlanNodePtr PlanMocker::buildWithCTE(CTEInfo & cte_info)
{
    if (!root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Optimizer MockedPlanNode Error, empty plan!");
    return MockedPlanNode::recursiveBuildSelf(root, context, cte_info);
}

PlanNodePtr MockedPlanNode::recursiveBuildSelf(MockedPlanNodePtr root, const ContextMutablePtr & context, CTEInfo & cte_info)
{
    PlanNodes children;
    for (const auto & child : root->children)
    {
        children.emplace_back(recursiveBuildSelf(child, context, cte_info));
    }

    CTEId cte_id = 0;
    if (root->cte_def)
    {
        // If the CTEDef already exists, reuse the CTEId, otherwise create a new one
        for (const auto & [id, def] : cte_info.getCTEs())
        {
            if (def->getId() == root->cte_def->getId())
            {
                cte_id = id;
                break;
            }
        }
        if (cte_id == 0)
            cte_info.add(cte_id = cte_info.nextCTEId(), root->cte_def);
    }

    auto step = root->handler(HandlerContext{children, context, cte_id});

    if (step->getType() == IQueryPlanStep::Type::Join)
    {
        if (children.size() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Optimizer MockedPlanNode Error, Join should use addChildren() with two arguments!");
    }
    else if (step->getType() != IQueryPlanStep::Type::Union && children.size() > 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Optimizer MockedPlanNode Error, only Join/Union can call addChildren()");
    }

    return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(step), children);
}


namespace CreateMockedPlanNode
{
    MockedPlanNodePtr values(const Names & columns, Fields values)
    {
        return MockedPlanNode::of([=](const HandlerContext &) mutable {
            Block header;
            for (const auto & name : columns)
            {
                ColumnWithTypeAndName column{std::make_shared<DataTypeString>(), name};
                header.insert(column);
            }

            if (values.empty())
            {
                values.assign(columns.size(), Field(""));
            }
            return std::make_shared<ValuesStep>(header, values, 0);
        });
    }

    MockedPlanNodePtr values(const Block & header)
    {
        return MockedPlanNode::of([=](const HandlerContext &) {
            Fields values;
            return std::make_shared<ValuesStep>(header, values, 0);
        });
    }

    // MockedPlanNodePtr tableScan(String database, String table, Names columns)
    // {
    // }

    MockedPlanNodePtr limit(size_t limit, size_t offset, bool partial)
    {
        return MockedPlanNode::of([=](const HandlerContext & c) {
            DataStream input_stream = c.inputs[0]->getStep()->getOutputStream();
            SizeOrVariable limit_size(limit);
            SizeOrVariable offset_size(offset);
            SortDescription sort_desc;
            return std::make_shared<LimitStep>(input_stream, limit_size, offset_size, false, false, sort_desc, partial);
        });
    }

    MockedPlanNodePtr projection(const Assignments & assignments, const NameToType & name_to_type, bool final_project)
    {
        return MockedPlanNode::of([=](const HandlerContext & c) {
            DataStream input_stream = c.inputs[0]->getStep()->getOutputStream();
            return std::make_shared<ProjectionStep>(input_stream, assignments, name_to_type, final_project);
        });
    }

    MockedPlanNodePtr projection(const NameSet & allow_names, bool final_project)
    {
        return MockedPlanNode::of([=](const HandlerContext & c) {
            DataStream input_stream = c.inputs[0]->getStep()->getOutputStream();
            Assignments assignments;
            NameToType name_to_type;

            for (const auto & [name, type] : input_stream.getNamesAndTypes())
            {
                if (allow_names.empty() || allow_names.contains(name))
                {
                    if (!assignments.contains(name))
                    {
                        assignments.emplace(name, std::make_shared<ASTIdentifier>(name));
                        name_to_type.emplace(name, type);
                    }
                }
            }

            return std::make_shared<ProjectionStep>(input_stream, assignments, name_to_type, final_project);
        });
    }

    MockedPlanNodePtr filter(ConstASTPtr filter)
    {
        return MockedPlanNode::of([=](const HandlerContext & c) {
            DataStream input_stream = c.inputs[0]->getStep()->getOutputStream();
            return std::make_shared<FilterStep>(input_stream, filter);
        });
    }

    MockedPlanNodePtr sorting(SortDescription description, size_t limit, SortingStep::Stage stage)
    {
        return MockedPlanNode::of([=](const HandlerContext & c) {
            return std::make_shared<SortingStep>(c.inputs[0]->getStep()->getOutputStream(), std::move(description), limit, stage, SortDescription{});
        });
    }

    MockedPlanNodePtr exchange(const ExchangeMode & mode, Partitioning schema, bool keep_order)
    {
        return MockedPlanNode::of([=](const HandlerContext & c) {
            DataStreams input_streams;
            for (const auto & input : c.inputs)
                input_streams.emplace_back(input->getStep()->getOutputStream());
            return std::make_shared<ExchangeStep>(input_streams, mode, schema, keep_order);
        });
    }

    MockedPlanNodePtr join(
        Names left_keys,
        Names right_keys,
        ConstASTPtr join_filter,
        ASTTableJoin::Kind kind,
        ASTTableJoin::Strictness strictness,
        std::vector<bool> key_ids_null_safe)
    {
        return MockedPlanNode::of([=](const HandlerContext & c) {
            DataStreams input_streams;
            ColumnsWithTypeAndName output_header;
            for (const auto & input : c.inputs)
            {
                input_streams.emplace_back(input->getStep()->getOutputStream());
                auto input_header = input_streams.back().header;
                output_header.insert(output_header.end(), input_header.begin(), input_header.end());
            }
            return std::make_shared<JoinStep>(
                input_streams,
                DataStream{.header = output_header},
                kind,
                strictness,
                1,
                false,
                left_keys,
                right_keys,
                key_ids_null_safe,
                join_filter);
        });
    }

    MockedPlanNodePtr unionn(DataStream output_stream, OutputToInputs output_to_inputs)
    {
        return MockedPlanNode::of([=](const HandlerContext & c) {
            DataStreams input_streams;
            for (const auto & input : c.inputs)
                input_streams.emplace_back(input->getStep()->getOutputStream());
            return std::make_shared<UnionStep>(input_streams, output_stream, output_to_inputs);
        });
    }

    MockedPlanNodePtr distinct(const Names & columns)
    {
        return MockedPlanNode::of([=](const HandlerContext & c) {
            DataStream input_stream = c.inputs[0]->getStep()->getOutputStream();
            SizeLimits set_size_limits;
            UInt64 limit_hint = 0;
            return std::make_shared<DistinctStep>(input_stream, set_size_limits, limit_hint, columns, false, true);
        });
    }

    MockedPlanNodePtr distinct(const SizeLimits & set_size_limits, UInt64 limit_hint, const Names & columns, bool pre_distinct)
    {
        return MockedPlanNode::of([=](const HandlerContext & c) {
            DataStream input_stream = c.inputs[0]->getStep()->getOutputStream();
            return std::make_shared<DistinctStep>(input_stream, set_size_limits, limit_hint, columns, pre_distinct, true);
        });
    }

    MockedPlanNodePtr aggregating(const Names & keys, AggregateDescriptions aggregates, bool final)
    {
        return MockedPlanNode::of([=](const HandlerContext & c) {
            DataStream input_stream = c.inputs[0]->getStep()->getOutputStream();

            NameSet keys_not_hashed;
            GroupingSetsParamsList list;
            return std::make_shared<AggregatingStep>(
                input_stream, keys, keys_not_hashed, aggregates, list, final);
        });
    }

    MockedPlanNodePtr cte(PlanNodePtr cte_def, std::unordered_map<String, String> output_columns, bool has_filter)
    {
        auto result = MockedPlanNode::of([=](const HandlerContext & c) mutable {
            if (output_columns.empty())
            {
                for (const auto & name : cte_def->getOutputNames())
                    output_columns.emplace(name, name);
            }
            return std::make_shared<CTERefStep>(cte_def->getStep()->getOutputStream(), c.cte_id, output_columns, has_filter);
        });

        if (!cte_def)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Optimizer MockedPlanNode Error, cte_def is nullptr!");
        result->setCTEDef(cte_def);
        return result;
    }

}

}
