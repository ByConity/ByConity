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

#include <unordered_map>

#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSerDerHelper.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/PlanSymbolReallocator.h>


namespace DB
{

CTERefStep::CTERefStep(DataStream output_, CTEId id_, std::unordered_map<String, String> output_columns_, bool has_filter_)
    : ISourceStep(std::move(output_)), id(id_), output_columns(std::move(output_columns_)), has_filter(has_filter_)
{
}

CTERefStep::CTERefStep(Block header_, CTEId id_, std::unordered_map<String, String> output_columns_, bool has_filter_)
    : ISourceStep(DataStream{.header = header_}), id(id_), output_columns(std::move(output_columns_)), has_filter(has_filter_)
{
}

std::shared_ptr<IQueryPlanStep> CTERefStep::copy(ContextPtr) const
{
    return std::make_shared<CTERefStep>(output_stream.value(), id, output_columns, has_filter);
}

std::shared_ptr<CTERefStep> CTERefStep::fromProto(const Protos::CTERefStep & proto, ContextPtr)
{
    auto base_output_header = ISourceStep::deserializeFromProtoBase(proto.query_plan_base());
    auto id = proto.id();
    auto output_columns = deserializeMapFromProto<String, String>(proto.output_columns());
    auto has_filter = proto.has_filter();
    auto step = std::make_shared<CTERefStep>(base_output_header, id, output_columns, has_filter);

    return step;
}

void CTERefStep::toProto(Protos::CTERefStep & proto, bool) const
{
    ISourceStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    proto.set_id(id);
    serializeMapToProto(output_columns, *proto.mutable_output_columns());
    proto.set_has_filter(has_filter);
}

std::shared_ptr<ProjectionStep> CTERefStep::toProjectionStep() const
{
    NamesAndTypes inputs;
    Assignments assignments;
    NameToType name_to_type;
    for (const auto & item : output_stream.value().header)
    {
        auto it = output_columns.find(item.name);
        if (it != output_columns.end())
        {
            assignments.emplace_back(item.name, std::make_shared<ASTIdentifier>(it->second));
            name_to_type.emplace(item.name, item.type);
            inputs.emplace_back(NameAndTypePair{it->second, item.type});
        }
    }
    return std::make_shared<ProjectionStep>(DataStream{inputs}, assignments, name_to_type);
}

PlanNodePtr CTERefStep::toInlinedPlanNode(CTEInfo & cte_info, ContextMutablePtr & context) const
{
    auto rewrite = PlanSymbolReallocator::reallocate(cte_info.getCTEDef(id), context);

    // create projection step to guarante output symbols
    NamesAndTypes inputs;
    Assignments assignments;
    NameToType name_to_type;
    for (const auto & item : output_stream.value().header)
    {
        auto it = output_columns.find(item.name);
        if (it != output_columns.end())
        {
            auto new_symbol = rewrite.mappings.find(it->second);
            if (new_symbol == rewrite.mappings.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "output_stream symbol not found in cte def: " + it->second);
            assignments.emplace_back(item.name, std::make_shared<ASTIdentifier>(new_symbol->second));
            name_to_type.emplace(item.name, item.type);
            inputs.emplace_back(NameAndTypePair{it->second, item.type});
        }
    }
    return PlanNodeBase::createPlanNode(
        context->nextNodeId(), std::make_shared<ProjectionStep>(DataStream{inputs}, assignments, name_to_type), {rewrite.plan_node});
}

std::unordered_map<String, String> CTERefStep::getReverseOutputColumns() const
{
    std::unordered_map<String, String> reverse;
    for (const auto & item : output_columns)
        reverse.emplace(item.second, item.first);
    return reverse;
}
}
