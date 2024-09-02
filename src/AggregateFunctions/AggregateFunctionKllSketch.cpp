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

#include <AggregateFunctions/AggregateFunctionCboFamily.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Statistics/Base64.h>
#include <Statistics/StatsKllSketchImpl.h>
#include "Columns/ColumnSketchBinary.h"

// TODO: use datasketches
namespace DB
{

template <typename T>
struct KllData
{
    // TODO: use statistics object
    // datasketches::kll_sketch<T> data_{};
    using EmbeddedType = std::conditional_t<std::is_same_v<T, UUID>, UInt128, T>;
    Statistics::StatsKllSketchImpl<EmbeddedType> data_;

    explicit KllData(UInt64 logK = DEFAULT_KLL_SKETCH_LOG_K) : data_(logK) {}

    void add(T value) { data_.update(value); }

    void merge(const KllData & rhs) { data_.merge(rhs.data_); }

    using BlobType = String;
    void write(WriteBuffer & buf) const
    {
        BlobType blob = data_.serialize();
        writeBinary(blob, buf);
    }

    void read(ReadBuffer & buf)
    {
        BlobType blob;
        readBinary(blob, buf);
        data_.deserialize(blob);
    }

    std::string getText() const { return data_.to_string(); }

    void insertResultInto(IColumn & to) const
    {
        auto blob = data_.serialize();
        static_cast<ColumnSketchBinary &>(to).insertData(blob.c_str(), blob.size());
    }

    static String getName() { return "kll"; }
};


template <template <typename> class Function>
AggregateFunctionPtr
createAggregateFunctionKllSketch(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    UInt64 logK;
    if (parameters.empty())
    {
        logK = DEFAULT_KLL_SKETCH_LOG_K;
    }
    else if (parameters.size() == 1)
    {
        logK = parameters[0].safeGet<UInt64>();
    }
    else
    {
        throw Exception("Aggregate function " + name + " requires 0 or 1 parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];

    // TODO: support most data_type
    if (isColumnedAsNumber(data_type))
    {
        res.reset(createWithNumericBasedType<Function>(*data_type, argument_types, logK));
    }

    if (!res)
        throw Exception(
            "Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

template <typename T>
struct FuncImpl
{
    using Func = AggregateFunctionCboFamily<KllData, T>;
};
template <typename T>
using Func = typename FuncImpl<T>::Func;


void registerAggregateFunctionKllSketch(AggregateFunctionFactory & factory)
{
    AggregateFunctionWithProperties functor;
    functor.creator = createAggregateFunctionKllSketch<Func>;
    factory.registerFunction("kll", functor);
}

}
