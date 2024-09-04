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

#pragma once

#include <type_traits>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeHelper.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Protos/plan_node_utils.pb.h>
#include <QueryPlan/Assignment.h>
#include "QueryPlan/QueryPlan.h"

namespace DB
{

namespace Protos
{
    class QueryPlanStep;
}

class DataStream;

class IQueryPlanStep;
using QueryPlanStepPtr = std::shared_ptr<IQueryPlanStep>;
using QueryPlanStepPtr = std::shared_ptr<IQueryPlanStep>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

class TableJoin;
using TableJoinPtr = std::shared_ptr<TableJoin>;

#define SERIALIZE_ENUM(ITEM, BUF) writeBinary(UInt8(ITEM), BUF);

#define DESERIALIZE_ENUM(TYPE, ITEM, BUF) \
    TYPE ITEM; \
    { \
        UInt8 tmp; \
        readBinary(tmp, BUF); \
        ITEM = TYPE(tmp); \
    }

template <typename Type>
void serializeEnum(const Type & item, WriteBuffer & buf)
{
    writeBinary(UInt8(item), buf);
}

template <typename Type>
void deserializeEnum(Type & item, ReadBuffer & buf)
{
    UInt8 tmp;
    readBinary(tmp, buf);
    item = Type(tmp);
}


template <typename T>
void serializeItemVector(const std::vector<T> & itemVec, WriteBuffer & buf)
{
    writeBinary(itemVec.size(), buf);
    for (const auto & item : itemVec)
        item.serialize(buf);
}

template <typename T>
std::vector<T> deserializeItemVector(ReadBuffer & buf)
{
    size_t s_size;
    readBinary(s_size, buf);

    std::vector<T> itemVec(s_size);
    for (size_t i = 0; i < s_size; ++i)
    {
        T item;
        item.deserialize(buf);
        itemVec[i] = item;
    }

    return itemVec;
}

void serializeBlock(const Block & block, WriteBuffer & buf);
void serializeBlockWithData(const Block & block, WriteBuffer & buf);
Block deserializeBlock(ReadBuffer & buf);

void serializeColumn(const ColumnPtr & column, const DataTypePtr & data_type, WriteBuffer & buf);
ColumnPtr deserializeColumn(ReadBuffer & buf);

void serializePlanStep(const QueryPlanStepPtr & step, WriteBuffer & buf);

QueryPlanStepPtr deserializePlanStep(ReadBuffer & buf, ContextPtr context);

void serializeHeaderToProto(const Block & block, Protos::Block & proto);
Block deserializeHeaderFromProto(const Protos::Block & proto);

void serializeAssignmentsToProto(const Assignments & assignment, Protos::Assignments & proto);
Assignments deserializeAssignmentsFromProto(const Protos::Assignments & proto);

namespace impl
{
    template <typename Type>
    inline constexpr bool is_protobuf_native_v = std::is_fundamental_v<Type> || std::is_same_v<Type, String>;

    template <class>
    inline constexpr bool always_false_v = false;

    template <typename>
    struct is_std_vector : std::false_type
    {
    };

    template <typename T, typename A>
    struct is_std_vector<std::vector<T, A>> : std::true_type
    {
    };
}

// assume proto type and obj is matched, won't check
template <typename Map, typename Key, typename Value, typename ProtoType>
void serializeMapToProtoImpl(const Map & obj, ProtoType & proto)
{
    // requires Key, Value are simple
    static_assert(std::is_fundamental_v<Key> || std::is_same_v<Key, String>, "not supported");

    for (auto & [key, value] : obj)
    {
        auto & proto_value = proto[key];
        if constexpr (impl::is_protobuf_native_v<Value>)
        {
            proto_value = value;
        }
        else if constexpr (impl::is_std_vector<Value>::value)
        {
            using VectorElement = typename Value::value_type;
            static_assert(impl::is_protobuf_native_v<VectorElement>, "not supported");
            for (const auto & v : value)
            {
                proto_value.add_values(v);
            }
        }
        else if constexpr (std::is_same_v<Value, DataTypePtr>)
        {
            serializeDataTypeToProto(value, proto_value);
        }
        else
        {
            static_assert(impl::always_false_v<Value>, "not implemented");
        }
    }
}

template <typename Key, typename Value, typename ProtoType>
void serializeMapToProto(const std::unordered_map<Key, Value> & obj, ProtoType & proto)
{
    using UnorderedMap = std::unordered_map<Key, Value>;
    serializeMapToProtoImpl<UnorderedMap, Key, Value, ProtoType>(obj, proto);
}

template <typename Key, typename Value, typename ProtoType>
void serializeOrderedMapToProto(const std::map<Key, Value> & obj, ProtoType & proto)
{
    using OrderedMap = std::map<Key, Value>;
    serializeMapToProtoImpl<OrderedMap, Key, Value, ProtoType>(obj, proto);
}

// assume proto type and obj is matched, won't check
template <typename Map, typename Key, typename Value, typename ProtoType>
auto deserializeMapFromProtoImpl(const ProtoType & proto) -> Map
{
    // requires Key, Value are simple
    static_assert(std::is_fundamental_v<Key> || std::is_same_v<Key, String>, "not supported");
    Map result;
    for (auto & [key, proto_value] : proto)
    {
        if constexpr (impl::is_protobuf_native_v<Value>)
        {
            result.emplace(key, proto_value);
        }
        else if constexpr (impl::is_std_vector<Value>::value)
        {
            Value vec(proto_value.values().begin(), proto_value.values().end());
            result.emplace(key, std::move(vec));
        }
        else if constexpr (std::is_same_v<Value, DataTypePtr>)
        {
            result.emplace(key, deserializeDataTypeFromProto(proto_value));
        }
        else
        {
            static_assert(impl::always_false_v<Value>, "not implemented");
        }
    }
    return result;
}

template <typename Key, typename Value, typename ProtoType>
auto deserializeMapFromProto(const ProtoType & proto) -> std::unordered_map<Key, Value>
{
    using UnorderedMap = std::unordered_map<Key, Value>;
    return deserializeMapFromProtoImpl<UnorderedMap, Key, Value>(proto);
}

template <typename Key, typename Value, typename ProtoType>
auto deserializeOrderedMapFromProto(const ProtoType & proto) -> std::map<Key, Value>
{
    using OrderedMap = std::map<Key, Value>;
    return deserializeMapFromProtoImpl<OrderedMap, Key, Value>(proto);
}

// this made for struct Array, struct Tuple and struct Map
template <typename T>
void serializeFieldVectorToProto(const T & field_vector, Protos::FieldVector & proto)
{
    static_assert(std::is_base_of_v<FieldVector, T>, "not a FieldVector, see Core/Field.h");
    for (auto & element : field_vector)
    {
        element.toProto(*proto.add_fields());
    }
}

// this made for struct Array, struct Tuple and struct Map
template <typename T>
T deserializeFieldVectorFromProto(const Protos::FieldVector & proto)
{
    static_assert(std::is_base_of_v<FieldVector, T>, "not a FieldVector, see Core/Field.h");
    T res;
    res.resize(proto.fields_size());
    for (int i = 0; i < proto.fields_size(); ++i)
    {
        res[i].fillFromProto(proto.fields(i));
    }
    return res;
}


void serializeAggregateFunctionToProto(
    AggregateFunctionPtr function, const Array & parameters, const DataTypes & arg_types, Protos::AggregateFunction & proto);

void serializeAggregateFunctionToProto(AggregateFunctionPtr function, const Array & parameters, Protos::AggregateFunction & proto);

std::tuple<AggregateFunctionPtr, Array, DataTypes> deserializeAggregateFunctionFromProto(const Protos::AggregateFunction & proto);

void serializeQueryPlanStepToProto(const QueryPlanStepPtr & step, Protos::QueryPlanStep & proto);
QueryPlanStepPtr deserializeQueryPlanStepFromProto(const Protos::QueryPlanStep & proto, ContextPtr context);

bool isPlanStepEqual(const IQueryPlanStep & a, const IQueryPlanStep & b);
UInt64 hashPlanStep(const IQueryPlanStep & step, bool ignore_output_stream);
}
