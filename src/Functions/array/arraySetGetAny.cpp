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
#include <Columns/ColumnNullable.h>
#include <Common/typeid_cast.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Set.h>
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
 *  arraySetGetAny(col, (1,2,3)) -> 1  if 1 is in column.
 *  if there is muliple element in column, return the first element it found, e.g. arraySetGetAny([1,2,3,4], (1,2)) -> 1
 *  if there is no value can be found, return 0
 **/

class FunctionArraySetGetAny : public IFunction
{
public:
    using ColumnArrays = std::vector<const ColumnArray *>;
    using ColumnSets = std::vector<const ColumnSet *>;
    using Bools = std::vector<bool>;

    static constexpr auto name = "arraySetGetAny";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionArraySetGetAny>(context);
    }

    explicit FunctionArraySetGetAny(ContextPtr &)
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
            return array_type->getNestedType();
        return std::make_shared<DataTypeNullable>(array_type->getNestedType());
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

        auto res_column = array->getData().cloneEmpty();
        bool is_nullable = false;
        if (res_column->isNullable())
        {
            is_nullable = true;
            res_column = static_cast<const ColumnNullable &>(*res_column).getNestedColumn().cloneEmpty();
        }
        auto res_map = ColumnUInt8::create();

        const Set & set = *(set_column->getData());

        switch(set.data.type)
        {
            case SetVariants::Type::EMPTY:
                break;
            case SetVariants::Type::bitmap64:
                break;
#define M(NAME)                                                         \
                case SetVariants::Type::NAME:                           \
                    if (is_nullable)                                    \
                        setGetImpl<true, std::decay_t<decltype(*set.data.NAME)>>(*set.data.NAME, *array, *res_column, *res_map, set); \
                    else                                                \
                        setGetImpl<false, std::decay_t<decltype(*set.data.NAME)>>(*set.data.NAME, *array, *res_column, *res_map, set); \
                    break;

            APPLY_FOR_SET_VARIANTS(M)
#undef M
        }

        if (array_const)
            return ColumnConst::create(ColumnNullable::create(std::move(res_column), std::move(res_map)), arguments[0].column->size());
        else
            return ColumnNullable::create(std::move(res_column), std::move(res_map));
    }

    template <bool Nullable, typename Method>
    inline void setGetImpl([[maybe_unused]] Method& method,  [[maybe_unused]] const ColumnArray& array,
                           [[maybe_unused]] IColumn & result, [[maybe_unused]] ColumnUInt8 & result_map, const Set & set) const
    {
        const DataTypes & data_types = set.getDataTypes();
        if (data_types.empty())
            throw Exception("Cannot find a valid set type", ErrorCodes::LOGICAL_ERROR);

        const IDataType & data_type = *(data_types[0]);
        WhichDataType to_type(data_type);

        if constexpr (std::is_same<typename Method::Key, UInt8>::value)
        {
            if (to_type.isInt8())
                return setGetNumberImpl<Nullable, Method, Int8>(method, array, result, result_map);
            else if (to_type.isUInt8())
                return setGetNumberImpl<Nullable, Method, UInt8>(method, array, result, result_map);
        }
        else if constexpr (std::is_same<typename Method::Key, UInt16>::value)
        {
            if (to_type.isInt16())
                return setGetNumberImpl<Nullable, Method, Int16>(method, array, result, result_map);
            else if (to_type.isUInt16())
                return setGetNumberImpl<Nullable, Method, UInt16>(method, array, result, result_map);
        }
        else if constexpr (std::is_same<typename Method::Key, UInt32>::value)
        {
            if (to_type.isInt32())
                return setGetNumberImpl<Nullable, Method, Int32>(method, array, result, result_map);
            else if (to_type.isUInt32())
                return setGetNumberImpl<Nullable, Method, UInt32>(method, array, result, result_map);
        }
        else if constexpr (std::is_same<typename Method::Key, UInt64>::value)
        {
            if (to_type.isInt64())
                return setGetNumberImpl<Nullable, Method, Int64>(method, array, result, result_map);
            else if (to_type.isUInt64())
                return setGetNumberImpl<Nullable, Method, UInt64>(method, array, result, result_map);
        }

        throw Exception("Not implement type " + data_type.getName() + " for function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    template <bool Nullable, typename Method, typename DataType>
    inline void setGetNumberImpl(Method & method, const ColumnArray & array, IColumn & result, ColumnUInt8 & result_map) const
    {
        ConstNullMapPtr null_map{};
        ColumnRawPtrs key_cols;
        key_cols.push_back(&array.getData());
        if(Nullable) extractNestedColumnsAndNullMap(key_cols, null_map);

        if (auto * result_column = typeid_cast<ColumnVector<DataType> *>(&result))
        {
            typename Method::State state(key_cols, {}, nullptr);
            typename ColumnVector<DataType>::Container & result_container = result_column->getData();
            ColumnUInt8::Container & result_map_container = result_map.getData();
            size_t rsize = array.size();
            const auto & offsets = array.getOffsets();
            size_t pre_offset = 0, cur_offset = 0;

            Arena arena(0);
            for (size_t i = 0; i<rsize; ++i)
            {
                // get current elems in this array input
                cur_offset = offsets[i];
                bool get_element = false;
                for (size_t j = pre_offset; j < cur_offset; ++j)
                {
                    if (Nullable && (*null_map)[j]) continue;
                    auto key_holder = state.getKeyHolder(j, arena);
                    if (method.data.has(key_holder))
                    {
                        result_container.push_back(key_holder);
                        get_element = true;
                        break;
                    }
                }
                if (!get_element)
                {
                    result_column->insertDefault();
                    result_map_container.push_back(1);
                }
                else
                    result_map_container.push_back(0);
                pre_offset = offsets[i];
            }
        }
    }

};

REGISTER_FUNCTION(ArraySetGetAny)
{
    factory.registerFunction<FunctionArraySetGetAny>();
}

}
