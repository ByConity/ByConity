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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionBitmapExpressionCalculation.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

struct Settings;

namespace ErrorCodes
{
    const extern int TYPE_MISMATCH;
    const extern int SIZES_OF_COLUMNS_DOESNT_MATCH;
    const extern int AGGREGATE_FUNCTION_THROW;
}

namespace
{

template <template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
IAggregateFunction * createWithSpecificType(const IDataType & argument_type, TArgs &&... args)
{
    WhichDataType which(argument_type);

    if (which.idx == TypeIndex::UInt8 || which.idx == TypeIndex::UInt16 ||
            which.idx == TypeIndex::UInt32 || which.idx == TypeIndex::UInt64)
        return new AggregateFunctionTemplate<UInt64, UInt64>(std::forward<TArgs>(args)...);
    else if (which.idx == TypeIndex::Int8 || which.idx == TypeIndex::Int16 ||
            which.idx == TypeIndex::Int32 || which.idx == TypeIndex::Int64)
        return new AggregateFunctionTemplate<Int64, Int64>(std::forward<TArgs>(args)...);
    else if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<String, String>(std::forward<TArgs>(args)...);

    return nullptr;
}

template<template <typename, typename> class Function>
AggregateFunctionPtr createAggregateFunctionBitMapCount(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 2 )
        throw Exception("AggregateFunction " + name + " need two arguments", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    String expression;
    if (!parameters.empty() && !parameters[0].tryGet<String>(expression))
        throw Exception("AggregateFunction " + name + " need String as 1st parameter", ErrorCodes::BAD_TYPE_OF_FIELD);

    UInt64 is_bitmap_execute = 0;
    if (parameters.size() > 1)
        parameters[1].tryGet<UInt64>(is_bitmap_execute);

    const DataTypePtr& data_type = argument_types[0];
    if (!WhichDataType(data_type).isNativeInt() && !WhichDataType(data_type).isString())
        throw Exception("AggregateFunction " + name + " need signed numeric type (Int16 or bigger) or a string type for its first argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    if (WhichDataType(data_type).isInt8())
        throw Exception("Int8 type is not recommended! Please use Int16 or bigger size number", ErrorCodes::BAD_TYPE_OF_FIELD);

    if (!isBitmap64(argument_types[1]))
        throw Exception("AggregateFunction " + name + " need BitMap type for its second argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res(createWithSpecificType<Function>(*data_type, argument_types, expression, is_bitmap_execute));

    // res.reset(createWithNumericType<Function>(*data_type, argument_types, expression, is_bitmap_execute));
    if (!res)
        throw Exception("Failed to create aggregate function  " + name, ErrorCodes::AGGREGATE_FUNCTION_THROW);

    return res;
}

template<template <typename, typename> class Function>
AggregateFunctionPtr createAggregateFunctionBitMapMultiCount(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 2 )
        throw Exception("AggregateFunction " + name + " need two arguments", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::vector<String> expressions;
    if (!parameters.empty())
    {
        for (size_t i = 0; i < parameters.size(); ++i)
        {
            String expression;
            if (!parameters[i].tryGet<String>(expression))
                throw Exception(fmt::format("AggregateFunction {} need String as its {} parameter", name, argPositionToSequence(i+1)), ErrorCodes::BAD_TYPE_OF_FIELD);
            expressions.push_back(expression);
        }
    }

    const DataTypePtr& data_type = argument_types[0];
    if (!WhichDataType(data_type).isNativeInt() && !WhichDataType(data_type).isString())
        throw Exception("AggregateFunction " + name + " need signed numeric type (Int16 or bigger) or a string type for its first argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    if (WhichDataType(data_type).isInt8())
        throw Exception("Int8 type is not recommended! Please use Int16 or bigger size number", ErrorCodes::BAD_TYPE_OF_FIELD);

    if (!isBitmap64(argument_types[1]))
        throw Exception("AggregateFunction " + name + " need BitMap type for its second argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res(createWithSpecificType<Function>(*data_type, argument_types, expressions));

    if (!res)
        throw Exception("Failed to create aggregate function  " + name, ErrorCodes::AGGREGATE_FUNCTION_THROW);

    return res;
}

template<template <typename, typename> class Function>
AggregateFunctionPtr createAggregateFunctionBitMapMultiCountWithDate(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 3 )
        throw Exception("AggregateFunction " + name + " need three arguments", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::vector<String> expressions;
    if (!parameters.empty())
    {
        for (size_t i = 0; i < parameters.size(); i++)
        {
            String expression;
            if (!parameters[i].tryGet<String>(expression))
                throw Exception(fmt::format("AggregateFunction {} need String as its {} parameter", name, argPositionToSequence(i+1)), ErrorCodes::BAD_TYPE_OF_FIELD);
            expressions.push_back(expression);
        }
    }

    const DataTypePtr& date_type = argument_types[0];
    if (!WhichDataType(date_type).isNativeInt())
        throw Exception("AggregateFunction " + name + " need signed numeric type (Int16 or bigger) for its first argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    const DataTypePtr& data_type = argument_types[1];
    if (!WhichDataType(data_type).isNativeInt() && !WhichDataType(data_type).isString())
        throw Exception("AggregateFunction " + name + " need signed numeric type (Int16 or bigger) or a string type for its second argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (!isBitmap64(argument_types[2]))
        throw Exception("AggregateFunction " + name + " need BitMap type for its third argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res(createWithSpecificType<Function>(*data_type, argument_types, expressions));

    if (!res)
        throw Exception("Failed to create aggregate function  " + name, ErrorCodes::AGGREGATE_FUNCTION_THROW);

    return res;
}

template<template <typename, typename> class Function>
AggregateFunctionPtr createAggregateFunctionBitMapExtract(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 2 )
        throw Exception("AggregateFunction " + name + " need two arguments", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    String expression;
    if (!parameters.empty() && !parameters[0].tryGet<String>(expression))
        throw Exception("AggregateFunction " + name + " need String as 1st parameter", ErrorCodes::BAD_TYPE_OF_FIELD);

    UInt64 is_bitmap_execute = 0;
    if (parameters.size() > 1)
        parameters[1].tryGet<UInt64>(is_bitmap_execute);

    const DataTypePtr& data_type = argument_types[0];
    if (!WhichDataType(data_type).isNativeInt() && !WhichDataType(data_type).isString())
        throw Exception("AggregateFunction " + name + " need signed numeric type (Int16 or bigger) or a string type for its first argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    if (WhichDataType(data_type).isInt8())
        throw Exception("Int8 type is not recommended! Please use Int16 or bigger size number", ErrorCodes::BAD_TYPE_OF_FIELD);

    if (!isBitmap64(argument_types[1]))
        throw Exception("AggregateFunction " + name + " need BitMap type for its second argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res(createWithSpecificType<Function>(*data_type, argument_types, expression, is_bitmap_execute));

    if (!res)
        throw Exception("Failed to create aggregate function  " + name, ErrorCodes::AGGREGATE_FUNCTION_THROW);

    return res;
}

template<template <typename, typename> class Function>
AggregateFunctionPtr createAggregateFunctionBitMapMultiExtract(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 2 )
        throw Exception("AggregateFunction " + name + " need two arguments", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::vector<String> expressions;
    if (!parameters.empty())
    {
        for (size_t i = 0; i < parameters.size(); i++)
        {
            String expression;
            if (!parameters[i].tryGet<String>(expression))
                throw Exception(fmt::format("AggregateFunction {} need String as its {} parameter", name, argPositionToSequence(i+1)), ErrorCodes::BAD_TYPE_OF_FIELD);
            expressions.push_back(expression);
        }
    }

    const DataTypePtr& data_type = argument_types[0];

    if (!WhichDataType(data_type).isNativeInt() && !WhichDataType(data_type).isString())
        throw Exception(
            "AggregateFunction " + name + " need signed numeric type (Int16 or bigger) or a string type for its first argument",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
        );

    if (WhichDataType(data_type).isInt8())
        throw Exception("Int8 type is not recommended! Please use Int16 or bigger size number", ErrorCodes::BAD_TYPE_OF_FIELD);

    if (!isBitmap64(argument_types[1]))
        throw Exception("AggregateFunction " + name + " need BitMap type for its second argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res(createWithSpecificType<Function>(*data_type, argument_types, expressions));

    return res;
}

template<template <typename, typename> class Function>
AggregateFunctionPtr createAggregateFunctionBitMapMultiExtractWithDate(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 3)
        throw Exception("AggregateFunction " + name + " need two arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    std::vector<String> expressions;
    if (!parameters.empty())
    {
        for (size_t i = 0; i < parameters.size(); i++)
        {
            String expression;
            if (!parameters[i].tryGet<String>(expression))
                throw Exception(fmt::format("AggregateFunction {} need String as its {} parameter", name, argPositionToSequence(i+1)), ErrorCodes::BAD_TYPE_OF_FIELD);
            expressions.push_back(expression);
        }
    }

    const DataTypePtr& date_type = argument_types[0];
    if(!WhichDataType(date_type).isNativeInt())
        throw Exception("AggregateFunction " + name + " need signed numeric type for its first argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    const DataTypePtr& data_type = argument_types[1];
    if (!WhichDataType(data_type).isNativeInt() && !WhichDataType(data_type).isString())
        throw Exception("AggregateFunction " + name + " need signed numeric type (Int16 or bigger) or a string type for its second argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (!isBitmap64(argument_types[2]))
        throw Exception("AggregateFunction " + name + " need BitMap type for its third argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res(createWithSpecificType<Function>(*data_type, argument_types, expressions));

    return res;
}

}

void registerAggregateFunctionsBitmapExpressionCalculation(AggregateFunctionFactory & factory)
{
    factory.registerFunction(
        "BitmapCount",
        createAggregateFunctionBitMapCount<AggregateFunctionBitMapCount>,
        AggregateFunctionFactory::CaseInsensitive
    );

    factory.registerFunction(
        "BitmapMultiCount",
        createAggregateFunctionBitMapMultiCount<AggregateFunctionBitMapMultiCount>,
        AggregateFunctionFactory::CaseInsensitive
    );

    factory.registerFunction(
        "BitmapMultiCountWithDate",
        createAggregateFunctionBitMapMultiCountWithDate<AggregateFunctionBitMapMultiCountWithDate>,
        AggregateFunctionFactory::CaseInsensitive
    );

    factory.registerFunction(
        "BitmapExtract",
        createAggregateFunctionBitMapExtract<AggregateFunctionBitMapExtract>,
        AggregateFunctionFactory::CaseInsensitive
    );

    factory.registerFunction(
        "BitmapMultiExtract",
        createAggregateFunctionBitMapMultiExtract<AggregateFunctionBitMapMultiExtract>,
        AggregateFunctionFactory::CaseInsensitive
    );

    factory.registerFunction(
        "BitmapMultiExtractWithDate",
        createAggregateFunctionBitMapMultiExtractWithDate<AggregateFunctionBitMapMultiExtractWithDate>,
        AggregateFunctionFactory::CaseInsensitive
    );

    factory.registerAlias(
        "BitmapCountV2", "BitmapCount", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias(
        "BitmapMultiCountV2", "BitmapMultiCount", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias(
        "BitmapMultiCountWithDateV2", "BitmapMultiCountWithDate", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias(
        "BitmapExtractV2", "BitmapExtract", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias(
        "BitmapMultiExtractV2", "BitmapMultiExtract", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias(
        "BitmapMultiExtractWithDateV2", "BitmapMultiExtractWithDate", AggregateFunctionFactory::CaseInsensitive);
}

}
