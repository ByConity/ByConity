/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionStringOrArrayToT.h>
#include <common/map.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Calculates the length of a string in bytes.
  */
struct LengthImpl
{
    static constexpr auto is_fixed_to_constant = true;

    static void vector(const ColumnString::Chars & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = offsets[i] - 1 - offsets[i - 1];
    }

    static void vectorFixedToConstant(const ColumnString::Chars & /*data*/, size_t n, UInt64 & res)
    {
        res = n;
    }

    static void vectorFixedToVector(const ColumnString::Chars & /*data*/, size_t /*n*/, PaddedPODArray<UInt64> & /*res*/)
    {
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = offsets[i] - offsets[i - 1];
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function length to UUID argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    [[noreturn]] static void ipv6(const ColumnIPv6::Container &, size_t &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function length to IPv6 argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    [[noreturn]] static void ipv4(const ColumnIPv4::Container &, size_t &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function length to IPv4 argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


struct NameLength
{
    static constexpr auto name = "length";
};

using FunctionLength = FunctionStringOrArrayToT<LengthImpl, NameLength, UInt64, false>;
struct NameArraySize
{
    static constexpr auto name = "arraySize";
};


using FunctionArraySize = FunctionStringOrArrayToT<LengthImpl, NameArraySize, UInt64, false, true>;


/// Also works with arrays.
class SizeOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "size";
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<SizeOverloadResolver>(context); }

    explicit SizeOverloadResolver(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (isArray(removeNullable(arguments.at(0).type)))
            return FunctionFactory::instance().getImpl("arraySize", context)->build(arguments);
        else
            return std::make_unique<FunctionToFunctionBaseAdaptor>(
                FunctionLength::create(context),
                collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (isArray(removeNullable(arguments.at(0))))
            return FunctionArraySize::create(context)->getReturnTypeImpl(arguments);
        else
            return FunctionLength::create(context)->getReturnTypeImpl(arguments);
    }

private:
    ContextPtr context;
};

REGISTER_FUNCTION(Length)
{
    factory.registerFunction<FunctionLength>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionArraySize>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("octet_length", NameLength::name, FunctionFactory::CaseInsensitive);

    factory.registerFunction<SizeOverloadResolver>(FunctionFactory::CaseInsensitive);
}

}
