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

#include <Functions/IFunction.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/typeid_cast.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Set.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnSet.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/NullableUtils.h>
#include <Core/Block.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

/**
 *  arraySetGet(col, (1,2,3)) -> [1,2]  if and only if 1, 2 is in column.
 *  if there is no value can be found, return empty array []
 **/

class FunctionArraySetGet : public IFunction
{
public:
    using ColumnArrays = std::vector<const ColumnArray *>;
    using ColumnSets = std::vector<const ColumnSet *>;
    using Bools = std::vector<bool>;

    static constexpr auto name = "arraySetGet";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionArraySetGet>(context);
    }

    explicit FunctionArraySetGet(ContextPtr)
    {
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForConstantFolding() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception("Argument for function " + getName() + " must be array.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (const DataTypeNullable * nullable_inner_type = checkAndGetDataType<DataTypeNullable>(array_type->getNestedType().get()))
            return std::make_shared<DataTypeArray>(nullable_inner_type->getNestedType());
        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /* input_rows_count */) const override
    {
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        const ColumnArray * array_const = checkAndGetColumnConstData<ColumnArray>(arguments[0].column.get());

        if (array_const)
            array = array_const;

        if (!array)
            throw Exception("Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
                            ErrorCodes::ILLEGAL_COLUMN);

        const ColumnSet * set_column = checkAndGetColumn<ColumnSet>(arguments[1].column.get());
        if (!set_column)
            throw Exception("Illegal column " + arguments[1].column->getName() + " of second argument of function " + getName(),
                            ErrorCodes::ILLEGAL_COLUMN);

        auto res = array->cloneEmpty();

        bool is_nullable = false;
        if (const auto *nullable_inner = typeid_cast<const ColumnNullable *>(&array->getData()))
        {
            is_nullable = true;
            res = ColumnArray::create(nullable_inner->getNestedColumn().cloneEmpty());
        }

        ColumnArray & res_array = static_cast<ColumnArray &>(*res);

        const Set & set = *(set_column->getData());

        switch(set.data.type)
        {
            case SetVariants::Type::EMPTY:
                break;
            // case SetVariants::Type::bitmap64:
                // break;
#define M(NAME)                                                         \
                case SetVariants::Type::NAME:                           \
                    if (is_nullable)                                    \
                        setGetImpl<true, std::decay_t<decltype(*set.data.NAME)>>(*set.data.NAME, *array, res_array, set); \
                    else                                                \
                        setGetImpl<false, std::decay_t<decltype(*set.data.NAME)>>(*set.data.NAME, *array, res_array, set); \
                    break;

            APPLY_FOR_SET_VARIANTS(M)
#undef M
        }

        if (array_const)
            return ColumnConst::create(std::move(res), arguments[0].column->size());
        else
            return res;
    }

    template <bool Nullable, typename Method>
    inline void setGetImpl([[maybe_unused]] Method& method,  [[maybe_unused]] const ColumnArray& array,
                           [[maybe_unused]] ColumnArray & result, const Set & set) const
    {
        const DataTypes & data_types = set.getDataTypes();
        if (data_types.empty())
            throw Exception("Cannot find a valid set type", ErrorCodes::LOGICAL_ERROR);

        const IDataType & data_type = *(data_types[0]);
        WhichDataType to_type(data_type);

        if constexpr (std::is_same<typename Method::Key, UInt8>::value)
        {
            if (to_type.isInt8())
                return setGetNumberImpl<Nullable, Method, Int8>(method, array, result);
            else if (to_type.isUInt8())
                return setGetNumberImpl<Nullable, Method, UInt8>(method, array, result);
        }
        else if constexpr (std::is_same<typename Method::Key, UInt16>::value)
        {
            if (to_type.isInt16())
                return setGetNumberImpl<Nullable, Method, Int16>(method, array, result);
            else if (to_type.isUInt16())
                return setGetNumberImpl<Nullable, Method, UInt16>(method, array, result);
        }
        else if constexpr (std::is_same<typename Method::Key, UInt32>::value)
        {
            if (to_type.isInt32())
                return setGetNumberImpl<Nullable, Method, Int32>(method, array, result);
            else if (to_type.isUInt32())
                return setGetNumberImpl<Nullable, Method, UInt32>(method, array, result);
        }
        else if constexpr (std::is_same<typename Method::Key, UInt64>::value)
        {
            if (to_type.isInt64())
                return setGetNumberImpl<Nullable, Method, Int64>(method, array, result);
            else if (to_type.isUInt64())
                return setGetNumberImpl<Nullable, Method, UInt64>(method, array, result);
        }

        throw Exception("Not implement type " + data_type.getName() + " for function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    template <bool Nullable, typename Method, typename DataType>
    inline void setGetNumberImpl(Method & method, const ColumnArray & array, ColumnArray & result) const
    {
        ConstNullMapPtr null_map{};
        ColumnRawPtrs key_cols;
        key_cols.push_back(&array.getData());
        if(Nullable) extractNestedColumnsAndNullMap(key_cols, null_map);

        if (auto * result_column = typeid_cast<ColumnVector<DataType> *>(&result.getData()))
        {
            typename Method::State state(key_cols, {}, nullptr);
            typename ColumnVector<DataType>::Container & result_container = result_column->getData();
            size_t rsize = array.size();
            const auto & offsets = array.getOffsets();
            auto & res_offsets = result.getOffsets();
            size_t pre_offset = 0, cur_offset = 0;

            Arena arena(0);
            for (size_t i = 0; i<rsize; ++i)
            {
                // get current elems in this array input
                cur_offset = offsets[i];
                size_t element_size = 0;
                for (size_t j = pre_offset; j < cur_offset; ++j)
                {
                    if (Nullable && (*null_map)[j]) continue;
                    auto key_holder = state.getKeyHolder(j, arena);
                    if (method.data.has(key_holder))
                    {
                        result_container.push_back(key_holder);
                        element_size++;
                    }
                }
                res_offsets.push_back(res_offsets.back() + element_size);
                pre_offset = offsets[i];
            }
        }
    }

};

REGISTER_FUNCTION(ArraySetGet)
{
    factory.registerFunction<FunctionArraySetGet>();
}

}
