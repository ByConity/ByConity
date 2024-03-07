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

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Protos/PreparedStatementHelper.h>
#include <Protos/plan_node_utils.pb.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

void InputOrderInfo::toProto(Protos::InputOrderInfo & proto) const
{
    for (const auto & element : order_key_prefix_descr)
        element.toProto(*proto.add_order_key_prefix_descr());
    proto.set_direction(direction);
}

std::shared_ptr<InputOrderInfo> InputOrderInfo::fromProto(const Protos::InputOrderInfo & proto)
{
    SortDescription order_key_prefix_descr;
    for (const auto & proto_element : proto.order_key_prefix_descr())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        order_key_prefix_descr.emplace_back(std::move(element));
    }
    auto direction = proto.direction();
    auto res = std::make_shared<InputOrderInfo>(std::move(order_key_prefix_descr), direction);

    return res;
}

void SelectQueryInfo::toProto(Protos::SelectQueryInfo & proto) const
{
    serializeASTToProto(query, *proto.mutable_query());
    serializeASTToProto(view_query, *proto.mutable_view_query());
    serializeASTToProto(partition_filter, *proto.mutable_partition_filter());
    if (input_order_info)
        input_order_info->toProto(*proto.mutable_input_order_info());
}

void SelectQueryInfo::fillFromProto(const Protos::SelectQueryInfo & proto)
{
    query = deserializeASTFromProto(proto.query());
    view_query = deserializeASTFromProto(proto.view_query());
    partition_filter = deserializeASTFromProto(proto.partition_filter());
    input_order_info = proto.has_input_order_info() ? InputOrderInfo::fromProto(proto.input_order_info()) : nullptr;
}

std::shared_ptr<InterpreterSelectQuery> SelectQueryInfo::buildQueryInfoFromQuery(ContextPtr context, const StoragePtr & storage, const String & query, SelectQueryInfo & query_info)
{
    ReadBufferFromString rb(query);
    ASTPtr query_ptr = deserializeAST(rb);
    auto interpreter = std::make_shared<InterpreterSelectQuery>(query_ptr, context, storage);
    query_info.query = query_ptr;
    query_info.syntax_analyzer_result = interpreter->syntax_analyzer_result;
    query_info.prewhere_info = interpreter->analysis_result.prewhere_info;
    query_info.sets = interpreter->query_analyzer->getPreparedSets();
    query_info.index_context = interpreter->query_analyzer->getIndexContext();
    return interpreter;
}

ASTPtr getFilterFromQueryInfo(const SelectQueryInfo & query_info, bool clone)
{
    const ASTSelectQuery & select = query_info.query->as<ASTSelectQuery &>();
    if (select.where())
        return clone ? select.where()->clone() : select.where();
    if (select.prewhere())
        return clone ? select.prewhere()->clone() : select.prewhere();
    if (!query_info.atomic_predicates_expr.empty())
    {
        ASTPtr filter_query;
        if (query_info.atomic_predicates_expr.size() == 1)
        {
            filter_query =  query_info.atomic_predicates_expr[0];
        }
        else
        {
            auto function = std::make_shared<ASTFunction>();
            function->name = "and";
            function->arguments = std::make_shared<ASTExpressionList>();
            function->children.push_back(function->arguments);
            for (const auto & expr : query_info.atomic_predicates_expr)
            {
                function->arguments->children.push_back(expr);
            }
            filter_query = filter_query ? makeASTFunction("and", std::move(filter_query), std::move(function)) : std::move(function);
        }
        return clone ? filter_query->clone() : filter_query;
    }
    return nullptr;
}

String AtomicPredicate::dump() const
{
    std::stringstream ss;
    if (predicate_actions)
        ss << "Predicate: \n" << predicate_actions->dumpDAG() << "\n";
    ss << "Filter column name: " << filter_column_name << "\n";
    ss << "Is row filter: " << is_row_filter << "\n";
    if (index_context)
        ss << "Index context: \n" << index_context->toString() << "\n";
    ss << "Remove filter column: " << remove_filter_column << "\n";
    return ss.str();
}

const PrewhereInfoPtr & getPrewhereInfo(const SelectQueryInfo & query_info)
{
    return query_info.projection ? query_info.projection->prewhere_info : query_info.prewhere_info;
}

const std::deque<AtomicPredicatePtr> & getAtomicPredicates(const SelectQueryInfo & query_info)
{
    return query_info.atomic_predicates;
}

MergeTreeIndexContextPtr getIndexContext(const SelectQueryInfo & query_info)
{
    /// Projection shouldn't have bitmap index
    return query_info.projection ? nullptr : query_info.index_context;
}

}
