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
#include <Protos/plan_node_utils.pb.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/InterpreterSelectQuery.h>

namespace DB
{

void InputOrderInfo::toProto(Protos::InputOrderInfo & proto) const
{
    for (const auto & element : order_key_prefix_descr)
        element.toProto(*proto.add_order_key_prefix_descr());
    proto.set_direction(direction);
}

std::shared_ptr<InputOrderInfo> InputOrderInfo::fromProto(const Protos::InputOrderInfo & proto, ContextPtr)
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
}

void SelectQueryInfo::fillFromProto(const Protos::SelectQueryInfo & proto)
{
    query = deserializeASTFromProto(proto.query());
    view_query = deserializeASTFromProto(proto.view_query());
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
    // query_info.index_context = interpreter->query_analyzer->getIndexContext();
    return interpreter;
}

}
