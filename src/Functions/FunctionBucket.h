#pragma once

#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVector.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <common/types.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <bool ModSplitNumberInside>
class FunctionBucket : public IExecutableFunction
{
public:
    static constexpr auto name = "bucket";

    explicit FunctionBucket(
        ExecutableFunctionPtr hash_function_,
        UInt64 bucket_size_,
        UInt64 is_with_range_,
        UInt64 split_number_)
        : hash_function(std::move(hash_function_))
        , bucket_size(bucket_size_)
        , is_with_range(is_with_range_)
        , split_number(split_number_)
        , split_number_argument(ColumnWithTypeAndName{})
    {
    }

    explicit FunctionBucket(
        ExecutableFunctionPtr hash_function_,
        UInt64 bucket_size_,
        UInt64 is_with_range_,
        UInt64 split_number_,
        ColumnWithTypeAndName split_number_argument_)
        : hash_function(std::move(hash_function_))
        , bucket_size(bucket_size_)
        , is_with_range(is_with_range_)
        , split_number(split_number_)
        , split_number_argument(std::move(split_number_argument_))
    {
    }

    std::string getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto result = ColumnUInt64::create(input_rows_count, 0);
        auto & result_data = result->getData();
        ColumnPtr hash_result;
        if constexpr (ModSplitNumberInside)
        {
            ColumnsWithTypeAndName full_args = arguments;
            full_args.emplace_back(split_number_argument);
            hash_result = hash_function->execute(full_args, result_type, input_rows_count, false);
        }
        else
        {
            hash_result = hash_function->execute(arguments, result_type, input_rows_count, false);
        }

        const auto * hash_result_ptr = typeid_cast<const ColumnVector<UInt64> *>(hash_result.get());
        if (!hash_result_ptr)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Function {} return unexpected type id: {}, it should return ColumnUInt64",
                hash_function->getName(),
                hash_result->getDataType());
        }
        auto & hash_data = (const_cast<ColumnVector<UInt64> *>(hash_result_ptr))->getData();
        if constexpr (!ModSplitNumberInside)
        {
            if (split_number > 0)
            {
                for (size_t i = 0; i < input_rows_count; i++)
                {
                    hash_data[i] = hash_data[i] % split_number;
                }
            }
        }

        if (!is_with_range)
        {
            for (size_t i = 0; i < input_rows_count; i++)
            {
                result_data[i] = hash_data[i] % bucket_size;
            }
        }
        else
        {
            auto shard_ratio = split_number / bucket_size;
            shard_ratio = shard_ratio == 0 ? 1 : shard_ratio;
            for (size_t i = 0; i < input_rows_count; i++)
            {   
                // implicit floor for shard ratio.
                // split_number has no constraint to match user requirement, so a shard_ratio(0), when split_number < bucket_size , is ok for customer.
                UInt64 bucket_number = hash_data[i] / shard_ratio;
                bucket_number = bucket_number >= bucket_size ? bucket_size - 1 : bucket_number;
                result_data[i] = bucket_number;
            }
        }

        return result;
    }

private:
    ExecutableFunctionPtr hash_function;
    UInt64 bucket_size;
    bool is_with_range;
    UInt64 split_number;
    ColumnWithTypeAndName split_number_argument;
};

class BucketFunctionBase : public IFunctionBase
{
public:
    static constexpr auto name = "bucket";
    BucketFunctionBase(DataTypes argument_types_, ContextPtr context_)
        : argument_types(std::move(argument_types_)), context(std::move(context_))
    {
    }

    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }

    virtual const DataTypePtr & getResultType() const override { return BucketFunctionBase::RESULT_DATA_TYPE; }

    virtual ExecutableFunctionPtr prepareWithParameters(const ColumnsWithTypeAndName & arguments, const Array & parameters) const override
    {
        if (parameters.size() != 4)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires 4 parameters", getName());
        }
        const String & hash_func_name = parameters[0].safeGet<String>();
        auto bucket_size = parameters[1].safeGet<UInt64>();
        if (bucket_size == 0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires positive bucket_size", getName());
        }
        auto is_with_range = parameters[2].safeGet<UInt64>();
        auto split_number = parameters[3].safeGet<UInt64>();

        FunctionOverloadResolverPtr hash_func_builder = FunctionFactory::instance().get(hash_func_name, context);

        if (hash_func_name == "dtspartition")
        {
            auto split_number_column
                = ColumnWithTypeAndName{ColumnInt64::create(1, split_number), BucketFunctionBase::SPLIT_NUMBER_TYPE, ""};
            auto full_args = arguments;
            full_args.emplace_back(split_number_column);
            FunctionBasePtr hash_func_base = hash_func_builder->build(full_args);
            auto executable_hash_func = hash_func_base->prepare(full_args);
            return std::make_unique<FunctionBucket<true>>(
                executable_hash_func, bucket_size, is_with_range, split_number, split_number_column);
        }

        FunctionBasePtr hash_func_base = hash_func_builder->build(arguments);
        auto executable_hash_func = hash_func_base->prepare(arguments);
        return std::make_unique<FunctionBucket<false>>(executable_hash_func, bucket_size, is_with_range, split_number);
    }

    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForConstantFolding() const override { return false; }

    static const DataTypePtr RESULT_DATA_TYPE;
    static const DataTypePtr SPLIT_NUMBER_TYPE;

private:
    DataTypes argument_types;
    ContextPtr context;
};


const DataTypePtr BucketFunctionBase::RESULT_DATA_TYPE = std::make_shared<DataTypeUInt64>();
const DataTypePtr BucketFunctionBase::SPLIT_NUMBER_TYPE = std::make_shared<DataTypeInt64>();

class FunctionBucketOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "bucket";

    explicit FunctionBucketOverloadResolver(ContextPtr context_) : context(std::move(context_)) { }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<FunctionBucketOverloadResolver>(std::move(context_));
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return BucketFunctionBase::RESULT_DATA_TYPE; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception(
                "Number of arguments for function " + getName() + " should be 1 or 2.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes arguments_types;

        for (const auto & arg : arguments)
        {
            arguments_types.push_back(arg.type);
        }
        return std::make_unique<BucketFunctionBase>(arguments_types, std::move(context));
    }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

private:
    ContextPtr context;
};

}
