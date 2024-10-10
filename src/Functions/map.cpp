#include <memory>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/MapHelpers.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/IFunction.h>
#include <Functions/array/arrayElement.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/executeSubQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/IStorage.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include "array/arrayIndex.h"
#include "Functions/like.h"
#include "Functions/FunctionsStringSearch.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}


namespace
{
    DataTypePtr tryMakeNullableForMapKeyValue(const DataTypePtr & type)
    {
        if (type->isNullable())
            return type;

        /// When type is low cardinality, its dictionary type may be nullable
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            if (low_cardinality_type->getDictionaryType()->isNullable())
                return type;
        }

        if (type->canBeInsideNullable())
            return makeNullable(type);

        /// Can't be inside nullable
        return type;
    }
}

// map(x, y, ...) is a function that allows you to make key-value pair
class FunctionMap : public IFunction
{
public:
    static constexpr auto name = "map";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionMap>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override
    {
        return false;
    }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() % 2 != 0)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires even number of arguments, but {} given", getName(), arguments.size());

        DataTypes keys, values;
        for (size_t i = 0; i < arguments.size(); i += 2)
        {
            keys.emplace_back(arguments[i]);
            values.emplace_back(arguments[i + 1]);
        }

        DataTypes tmp;
        tmp.emplace_back(removeNullable(getLeastSupertype(keys)));
        /// value type is allowed to be nullable in map
        tmp.emplace_back(getLeastSupertype(values));
        return std::make_shared<DataTypeMap>(tmp);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        size_t num_elements = arguments.size();

        if (num_elements == 0)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();
        /// try wrapped in nullable if possible
        const DataTypePtr & nullable_key_type = tryMakeNullableForMapKeyValue(key_type);
        const DataTypePtr & nullable_value_type = tryMakeNullableForMapKeyValue(value_type);

        Columns columns_holder(num_elements);
        ColumnRawPtrs column_ptrs(num_elements);

        for (size_t i = 0; i < num_elements; ++i)
        {
            const auto & arg = arguments[i];
            /// in order to support null values in map function, we need to cast arguments to nullable type
            const auto to_type = i % 2 == 0 ? nullable_key_type : nullable_value_type;

            ColumnPtr preprocessed_column = castColumn(arg, to_type);
            preprocessed_column = preprocessed_column->convertToFullColumnIfConst();

            columns_holder[i] = std::move(preprocessed_column);
            column_ptrs[i] = columns_holder[i].get();
        }

        /// Create and fill the result map.

        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        size_t total_elements = input_rows_count * num_elements / 2;
        keys_data->reserve(total_elements);
        values_data->reserve(total_elements);
        offsets->reserve(input_rows_count);

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (size_t j = 0; j < num_elements; j += 2)
            {
                auto key = (*column_ptrs[j])[i];
                auto value = (*column_ptrs[j + 1])[i];
                /// Skip null keys since Nullable type does not supported in map
                if (key.isNull())
                    continue;

                ++current_offset;
                keys_data->insert(key);
                values_data->insert(value);
            }

            offsets->insert(current_offset);
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}),
            std::move(offsets));

        return ColumnMap::create(nested_column);
    }
};

/// mapFromArrays(keys, values) is a function that allows you to make key-value pair from a pair of arrays
class FunctionMapFromArrays : public IFunction
{
public:
    static constexpr auto name = "mapFromArrays";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionMapFromArrays>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2 arguments, but {} given",
                getName(),
                arguments.size());

        const auto * keys_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!keys_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be an Array", getName());

        const auto * values_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!values_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be an Array", getName());

        DataTypes key_value_types{keys_type->getNestedType(), values_type->getNestedType()};
        return std::make_shared<DataTypeMap>(key_value_types);
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        ColumnPtr holder_keys;
        bool is_keys_const = isColumnConst(*arguments[0].column);
        const ColumnArray * col_keys;
        if (is_keys_const)
        {
            holder_keys = arguments[0].column->convertToFullColumnIfConst();
            col_keys = checkAndGetColumn<ColumnArray>(holder_keys.get());
        }
        else
        {
            col_keys = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        }

        ColumnPtr holder_values;
        bool is_values_const = isColumnConst(*arguments[1].column);
        const ColumnArray * col_values;
        if (is_values_const)
        {
            holder_values = arguments[1].column->convertToFullColumnIfConst();
            col_values = checkAndGetColumn<ColumnArray>(holder_values.get());
        }
        else
        {
            col_values = checkAndGetColumn<ColumnArray>(arguments[1].column.get());
        }

        if (!col_keys || !col_values)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Arguments of function {} must be array", getName());

        if (!col_keys->hasEqualOffsets(*col_values))
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH, "Array arguments for function {} must have equal sizes", getName());

        const auto & data_keys = col_keys->getDataPtr();
        const auto & data_values = col_values->getDataPtr();
        const auto & offsets = col_keys->getOffsetsPtr();
        auto nested_column = ColumnArray::create(ColumnTuple::create(Columns{data_keys, data_values}), offsets);
        return ColumnMap::create(nested_column);
    }
};

class FunctionMapContainsKeyLike : public IFunction
{
public:
    static constexpr auto name = "mapContainsKeyLike";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionMapContainsKeyLike>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*info*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        bool is_const = isColumnConst(*arguments[0].column);
        const ColumnMap * col_map = is_const ? checkAndGetColumnConstData<ColumnMap>(arguments[0].column.get())
                                             : checkAndGetColumn<ColumnMap>(arguments[0].column.get());
        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        if (!col_map || !map_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a map", getName());

        auto col_res = ColumnVector<UInt8>::create();
        typename ColumnVector<UInt8>::Container & vec_res = col_res->getData();

        if (input_rows_count == 0)
            return col_res;

        vec_res.resize(input_rows_count);

        const auto & column_array = typeid_cast<const ColumnArray &>(col_map->getNestedColumn());
        const auto & column_tuple = typeid_cast<const ColumnTuple &>(column_array.getData());

        const ColumnString * column_string = checkAndGetColumn<ColumnString>(column_tuple.getColumn(0));
        const ColumnFixedString * column_fixed_string = checkAndGetColumn<ColumnFixedString>(column_tuple.getColumn(0));

        FunctionLike func_like;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t element_start_row = row != 0 ? column_array.getOffsets()[row-1] : 0;
            size_t elem_size = column_array.getOffsets()[row]- element_start_row;

            ColumnPtr sub_map_column;
            DataTypePtr data_type;

            //The keys of one row map will be processed as a single ColumnString
            if (column_string)
            {
               sub_map_column = column_string->cut(element_start_row, elem_size);
               data_type = std::make_shared<DataTypeString>();
            }
            else
            {
               sub_map_column = column_fixed_string->cut(element_start_row, elem_size);
               data_type = std::make_shared<DataTypeFixedString>(checkAndGetColumn<ColumnFixedString>(sub_map_column.get())->getN());
            }

            size_t col_key_size = sub_map_column->size();
            auto column = is_const ? ColumnConst::create(std::move(sub_map_column), std::move(col_key_size)) : std::move(sub_map_column);

            ColumnsWithTypeAndName new_arguments =
                {
                    {
                        column,
                        data_type,
                        ""
                    },
                    arguments[1]
                };

            auto res = func_like.executeImpl(new_arguments, result_type, input_rows_count);
            const auto & container = checkAndGetColumn<ColumnUInt8>(res.get())->getData();

            const auto it = std::find_if(container.begin(), container.end(), [](int element){ return element == 1; });  // NOLINT
            vec_res[row] = it == container.end() ? 0 : 1;
        }

        return col_res;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be 2",
                            getName(), arguments.size());

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        const DataTypeString * pattern_type = checkAndGetDataType<DataTypeString>(arguments[1].type.get());

        if (!map_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a Map", getName());
        if (!pattern_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be String", getName());

        if (!isStringOrFixedString(map_type->getKeyType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Key type of map for function {} must be `String` or `FixedString`", getName());

        return std::make_shared<DataTypeUInt8>();
    }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
};

class FunctionExtractKeyLike : public IFunction
{
public:
    static constexpr auto name = "mapExtractKeyLike";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionExtractKeyLike>(); }

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*info*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2",
                getName(), arguments.size());


        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());

        if (!map_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a map", getName());


        auto key_type = map_type->getKeyType();

        WhichDataType which(key_type);

        if (!which.isStringOrFixedString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}only support the map with String or FixedString key",
                getName());

        if (!isStringOrFixedString(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument passed to function {} must be String or FixedString", getName());

        return std::make_shared<DataTypeMap>(map_type->getKeyType(), map_type->getValueType());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        bool is_const = isColumnConst(*arguments[0].column);
        const ColumnMap * col_map = typeid_cast<const ColumnMap *>(arguments[0].column.get());

        //It may not be necessary to check this condition, cause it will be checked in getReturnTypeImpl function
        if (!col_map)
            return nullptr;

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        auto key_type = map_type->getKeyType();
        auto value_type = map_type->getValueType();

        const auto & nested_column = col_map->getNestedColumn();
        const auto & keys_column = col_map->getNestedData().getColumn(0);
        const auto & values_column = col_map->getNestedData().getColumn(1);
        const ColumnString * keys_string_column = checkAndGetColumn<ColumnString>(keys_column);
        const ColumnFixedString * keys_fixed_string_column = checkAndGetColumn<ColumnFixedString>(keys_column);

        FunctionLike func_like;

        //create result data
        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        IColumn::Offset current_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t element_start_row = row != 0 ? nested_column.getOffsets()[row-1] : 0;
            size_t element_size = nested_column.getOffsets()[row]- element_start_row;

            ColumnsWithTypeAndName new_arguments;
            ColumnPtr sub_map_column;
            DataTypePtr data_type;

            if (keys_string_column)
            {
                sub_map_column = keys_string_column->cut(element_start_row, element_size);
                data_type = std::make_shared<DataTypeString>();
            }
            else
            {
                sub_map_column = keys_fixed_string_column->cut(element_start_row, element_size);
                data_type =std::make_shared<DataTypeFixedString>(checkAndGetColumn<ColumnFixedString>(sub_map_column.get())->getN());
            }

            size_t col_key_size = sub_map_column->size();
            auto column = is_const? ColumnConst::create(std::move(sub_map_column), std::move(col_key_size)) : std::move(sub_map_column);

            new_arguments = {
                    {
                        column,
                        data_type,
                        ""
                        },
                    arguments[1]
                    };

            auto res = func_like.executeImpl(new_arguments, result_type, input_rows_count);
            const auto & container = checkAndGetColumn<ColumnUInt8>(res.get())->getData();

            for (size_t row_num = 0; row_num < element_size; ++row_num)
            {
                if (container[row_num] == 1)
                {
                    auto key_ref = keys_string_column ?
                                   keys_string_column->getDataAt(element_start_row + row_num) :
                                   keys_fixed_string_column->getDataAt(element_start_row + row_num);
                    auto value_ref = values_column.getDataAt(element_start_row + row_num);

                    keys_data->insertData(key_ref.data, key_ref.size);
                    values_data->insertData(value_ref.data, value_ref.size);
                    current_offset += 1;
                }
            }

            offsets->insert(current_offset);
        }

        auto result_nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}),
            std::move(offsets));

        return ColumnMap::create(result_nested_column);
    }
};

class FunctionMapUpdate : public IFunction
{
public:
    static constexpr auto name = "mapUpdate";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionMapUpdate>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2",
                getName(), arguments.size());

        const DataTypeMap * left = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        const DataTypeMap * right = checkAndGetDataType<DataTypeMap>(arguments[1].type.get());

        if (!left || !right)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The two arguments for function {} must be both Map type",
                getName());
        if (!left->getKeyType()->equals(*right->getKeyType()) || !left->getValueType()->equals(*right->getValueType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The Key And Value type of Map for function {} must be the same",
                getName());

        return std::make_shared<DataTypeMap>(left->getKeyType(), left->getValueType());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnMap * col_map_left = typeid_cast<const ColumnMap *>(arguments[0].column.get());
        const auto * col_const_map_left = checkAndGetColumnConst<ColumnMap>(arguments[0].column.get());
        bool col_const_map_left_flag = false;
        if (col_const_map_left)
        {
            col_const_map_left_flag = true;
            col_map_left = typeid_cast<const ColumnMap *>(&col_const_map_left->getDataColumn());
        }
        if (!col_map_left)
            return nullptr;

        const ColumnMap * col_map_right = typeid_cast<const ColumnMap *>(arguments[1].column.get());
        const auto * col_const_map_right = checkAndGetColumnConst<ColumnMap>(arguments[1].column.get());
        bool col_const_map_right_flag = false;
        if (col_const_map_right)
        {
            col_const_map_right_flag = true;
            col_map_right = typeid_cast<const ColumnMap *>(&col_const_map_right->getDataColumn());
        }
        if (!col_map_right)
            return nullptr;

        const auto & nested_column_left = col_map_left->getNestedColumn();
        const auto & keys_data_left = col_map_left->getNestedData().getColumn(0);
        const auto & values_data_left = col_map_left->getNestedData().getColumn(1);
        const auto & offsets_left = nested_column_left.getOffsets();

        const auto & nested_column_right = col_map_right->getNestedColumn();
        const auto & keys_data_right = col_map_right->getNestedData().getColumn(0);
        const auto & values_data_right = col_map_right->getNestedData().getColumn(1);
        const auto & offsets_right = nested_column_right.getOffsets();

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();
        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        IColumn::Offset current_offset = 0;
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            size_t left_it_begin = col_const_map_left_flag ? 0 : offsets_left[row_idx - 1];
            size_t left_it_end = col_const_map_left_flag ? offsets_left.size() : offsets_left[row_idx];
            size_t right_it_begin = col_const_map_right_flag ? 0 : offsets_right[row_idx - 1];
            size_t right_it_end = col_const_map_right_flag ? offsets_right.size() : offsets_right[row_idx];

            for (size_t i = left_it_begin; i < left_it_end; ++i)
            {
                bool matched = false;
                auto key = keys_data_left.getDataAt(i);
                for (size_t j = right_it_begin; j < right_it_end; ++j)
                {
                    if (keys_data_right.getDataAt(j).toString() == key.toString())
                    {
                        matched = true;
                        break;
                    }
                }
                if (!matched)
                {
                    keys_data->insertFrom(keys_data_left, i);
                    values_data->insertFrom(values_data_left, i);
                    ++current_offset;
                }
            }

            for (size_t j = right_it_begin; j < right_it_end; ++j)
            {
                keys_data->insertFrom(keys_data_right, j);
                values_data->insertFrom(values_data_right, j);
                ++current_offset;
            }

            offsets->insert(current_offset);
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}),
            std::move(offsets));

        return ColumnMap::create(nested_column);
    }
};

struct NameMapContains { static constexpr auto name = "mapContains"; };

class FunctionMapContains : public IFunction
{
public:
    static constexpr auto name = "mapContains";

    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionMapContains>(); }

    String getName() const override
    {
        return NameMapContains::name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());

        if (!map_type)
            throw Exception{"First argument for function " + getName() + " must be a map",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        auto key_type = map_type->getKeyType();

        if (!(isNumber(arguments[1].type) && isNumber(key_type))
            && key_type->getName() != arguments[1].type->getName())
            throw Exception{"Second argument for function " + getName() + " must be a " + key_type->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        bool is_const = isColumnConst(*arguments[0].column);
        const ColumnMap * col_map = is_const ? checkAndGetColumnConstData<ColumnMap>(arguments[0].column.get()) : checkAndGetColumn<ColumnMap>(arguments[0].column.get());
        if (!col_map)
            throw Exception{"First argument for function " + getName() + " must be a map", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        const auto & nested_column = col_map->getNestedColumn();
        const auto & keys_data = col_map->getNestedData().getColumn(0);

        /// Prepare arguments to call arrayIndex for check has the array element.
        ColumnPtr column_array = ColumnArray::create(keys_data.getPtr(), nested_column.getOffsetsPtr());
        ColumnsWithTypeAndName new_arguments =
        {
            {
                is_const ? ColumnConst::create(std::move(column_array), keys_data.size()) : std::move(column_array),
                std::make_shared<DataTypeArray>(result_type),
                ""
            },
            arguments[1]
        };

        return FunctionArrayIndex<HasAction, NameMapContains>(nullptr).executeImpl(new_arguments, result_type, input_rows_count);
    }
};

class FunctionMapKeys : public IFunction
{
public:
    static constexpr auto name = "mapKeys";

    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionMapKeys>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());

        if (!map_type)
            throw Exception{"First argument for function " + getName() + " must be a map",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        auto key_type = map_type->getKeyType();

        return std::make_shared<DataTypeArray>(key_type);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const ColumnMap * col_map = typeid_cast<const ColumnMap *>(arguments[0].column.get());
        if (!col_map)
            return nullptr;

        const auto & nested_column = col_map->getNestedColumn();
        const auto & keys_data = col_map->getNestedData().getColumn(0);

        return ColumnArray::create(keys_data.getPtr(), nested_column.getOffsetsPtr());
    }
};


class FunctionMapValues : public IFunction
{
public:
    static constexpr auto name = "mapValues";

    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionMapValues>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());

        if (!map_type)
            throw Exception{"First argument for function " + getName() + " must be a map",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        auto value_type = map_type->getValueType();

        return std::make_shared<DataTypeArray>(value_type);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const ColumnMap * col_map = typeid_cast<const ColumnMap *>(arguments[0].column.get());
        if (!col_map)
            return nullptr;

        const auto & nested_column = col_map->getNestedColumn();
        const auto & values_data = col_map->getNestedData().getColumn(1);

        return ColumnArray::create(values_data.getPtr(), nested_column.getOffsetsPtr());
    }
};

/**
 * mapElement(map, key) is a function that allows you to retrieve a column from map
 * How to implement it depends on the storage model of map type
 *  - option 1: if map is simply serialized lob, and this function need to get the
 *    deserialized map type, and access element correspondingly
 *
 *  - option 2: if map is stored as expanded implicit column, and per key's value is
 *    stored as single file, this functions could be intepreted as implicited column
 *    ref, and more efficient.
 *
 * Option 2 will be used in TEA project, but we go to option 1 firstly for demo, and
 * debug end-to-end prototype.
 *
 */
class FunctionMapElement : public IFunction
{
public:
    static constexpr auto name = "mapElement";

    static FunctionPtr create(const ContextPtr &) { return std::make_shared<FunctionMapElement>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// we must handle null arguments here, since return type is nullable, but
    /// IExecutableFunction::defaultImplementationForNulls need to unwrap return type,
    /// which may cause return type mismatch.
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        /// handle null constant value in arguments
        NullPresence null_presence = getNullPresense(arguments);
        if (null_presence.has_null_constant)
            return makeNullable(std::make_shared<DataTypeNothing>());

        const DataTypeMap * map = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());

        if (!map)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function '{}' must be map, got '{}' instead",
                getName(),
                arguments[0].type->getName());

        return map->getValueTypeForImplicitColumn();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// handle null constant value in arguments
        NullPresence null_presence = getNullPresense(arguments);
        if (null_presence.has_null_constant)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const auto * col_map = checkAndGetColumn<ColumnMap>(arguments[0].column.get());
        const auto * col_const_map = checkAndGetColumnConst<ColumnMap>(arguments[0].column.get());
        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        if ((!col_map && !col_const_map) || !map_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "First argument for function '{}' must be map, got '{}' instead",
                getName(),arguments[0].type->getName());

        if (col_const_map)
            col_map = typeid_cast<const ColumnMap *>(&col_const_map->getDataColumn());

        auto & key_column = col_map->getKey();
        auto & value_column = col_map->getValue();
        auto & offsets = col_map->getOffsets();

        /// At first step calculate indices in array of values for requested keys.
        auto indices_column = DataTypeNumber<UInt64>().createColumn();
        indices_column->reserve(input_rows_count);
        auto & indices_data = assert_cast<ColumnVector<UInt64> &>(*indices_column).getData();

        bool executed = false;
        if (!isColumnConst(*arguments[1].column))
        {
            executed = FunctionArrayElement::matchKeyToIndexNumber(key_column, offsets, !!col_const_map, *arguments[1].column, indices_data)
                || FunctionArrayElement::matchKeyToIndexString(key_column, offsets, !!col_const_map, *arguments[1].column, indices_data);
        }
        else
        {
            Field index = (*arguments[1].column)[0];

            /// try convert const index to right type
            auto index_lit = std::make_shared<ASTLiteral>(index);
            Field key_field = tryConvertToMapKeyField(map_type->getKeyType(), index_lit->getColumnName());
            if (!key_field.isNull())
                index = key_field;

            executed = FunctionArrayElement::matchKeyToIndexNumberConst(key_column, offsets, index, indices_data)
                || FunctionArrayElement::matchKeyToIndexStringConst(key_column, offsets, index, indices_data);
        }
        if (!executed)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal types of arguments: {}, {} for function {}",
                arguments[0].type->getName(), arguments[1].type->getName(), getName());

        auto col_res = IColumn::mutate(result_type->createColumn());
        auto & col_res_ref = typeid_cast<ColumnNullable &>(*col_res);
        bool add_nullable = !value_column.lowCardinality() && value_column.canBeInsideNullable();
        for (size_t i = 0, size = indices_data.size(); i < size; ++i)
        {
            auto offset = col_const_map ? 0: offsets[i - 1];
            if (indices_data[i] == 0)
                col_res_ref.insert(Null());
            else
            {
                if (!add_nullable)
                    col_res_ref.insertFrom(value_column, offset + indices_data[i] - 1);
                else
                {
                    col_res_ref.getNestedColumn().insertFrom(value_column, offset + indices_data[i] - 1);
                    col_res_ref.getNullMapData().push_back(0);
                }
            }
        }

        return col_res;
    }
};

class FunctionGetMapKeys : public IFunction
{
public:
    static constexpr auto name = "getMapKeys";

    static FunctionPtr create(const ContextPtr & context) { return std::make_shared<FunctionGetMapKeys>(context); }

    FunctionGetMapKeys(const ContextPtr & c) : context(c) { }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    /// As we overrided executeImplDryRun and return empty result, constant folding of this funtion will got empty result.
    bool isSuitableForConstantFolding() const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }

    // function may output large-sized array, see also https://meego.larkoffice.com/clickhousech/story/detail/5285221241
    bool isSuitableForConstantFoldingInOptimizer() const override
    {
        return false;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4 && arguments.size() != 5)
            throw Exception(
                "Function " + getName()
                    + " requires 3 or 4 or 5 parameters: db, table, column, [partition expression], [max execute time]. Passed "
                    + toString(arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (unsigned int i = 0; i < (arguments.size() == 5 ? 4 : arguments.size()); i++)
        {
            const IDataType * argument_type = arguments[i].type.get();
            const DataTypeString * argument = checkAndGetDataType<DataTypeString>(argument_type);
            if (!argument)
                throw Exception(
                    "Illegal column " + arguments[i].name + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        if (arguments.size() == 5)
        {
            bool ok = checkAndGetDataType<DataTypeUInt64>(arguments[4].type.get())
                || checkAndGetDataType<DataTypeUInt32>(arguments[4].type.get())
                || checkAndGetDataType<DataTypeUInt16>(arguments[4].type.get())
                || checkAndGetDataType<DataTypeUInt8>(arguments[4].type.get());
            if (!ok)
                throw Exception(
                    "Illegal column " + arguments[4].name + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    inline String getColumnStringValue(const ColumnWithTypeAndName & argument) const { return argument.column->getDataAt(0).toString(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        String db_name = getColumnStringValue(arguments[0]);
        String table_name = getColumnStringValue(arguments[1]);
        String column_name = getColumnStringValue(arguments[2]);
        if (db_name.empty() || table_name.empty() || column_name.empty())
            throw Exception("Bad arguments: database/table/column should not be empty", ErrorCodes::BAD_ARGUMENTS);

        String pattern;
        if (arguments.size() >= 4)
        {
            pattern = getColumnStringValue(arguments[3]);
        }

        /// Check byte map type
        auto table_id = context->resolveStorageID({db_name, table_name});
        auto table = DatabaseCatalog::instance().getTable(table_id, context);
        auto metadata_snapshot = table->getInMemoryMetadataPtr();
        if (!metadata_snapshot || !metadata_snapshot->columns.hasPhysical(column_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {}.{} doesn't contain column {}", db_name, table_name, column_name);
        auto type = metadata_snapshot->columns.getPhysical(column_name).type;
        if (!type->isMap() || type->isKVMap())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Function getMapKeys must apply to ByteMap but given {}", type->isKVMap() ? "KV map" : type->getName());

        /**
            SELECT groupUniqArrayArray(ks) AS keys FROM (
                SELECT arrayMap(t -> t.2, arrayFilter(t -> t.1 = 'some_map', _map_column_keys)) AS ks
                FROM some_db.some_table WHERE match(_partition_id, '.*2020.*10.*10.*')
            ) SETTINGS early_limit_for_map_virtual_columns = 1, max_threads = 1
        */
        String inner_query = "SELECT arrayMap(t -> t.2, arrayFilter(t -> t.1 = '" + column_name + "', _map_column_keys)) AS ks" //
            + " FROM `" + db_name + "`.`" + table_name + "`" //
            + (pattern.empty() ? "" : " WHERE match(_partition_id, '" + pattern + "')");
        String query = "SELECT groupUniqArrayArray(ks) AS keys FROM ( " + inner_query
            + " ) SETTINGS early_limit_for_map_virtual_columns = 1, max_threads = 1";

        auto query_context = createContextForSubQuery(context);
        auto res = executeSubQueryWithOneRow(query, query_context, true);
        if (res)
        {
            // TODO(shiyuze): maybe add a new function to get result in different rows, just like arrayJoin(getMapKeys(xxx))

            /// Total map key number in result may exceed max_array_size_as_field (for example, 
            /// each pratition or bucket has different key sets), so we need to avoid calling 
            /// ColumnArray::[] or ColumnArray::get() to get array as a Field here.
            return ColumnConst::create(res.getByName("keys").column, input_rows_count)->convertToFullColumnIfConst();
        }
        else
        {
            return result_type->createColumnConst(input_rows_count, Array{})->convertToFullColumnIfConst();
        }
    }

    /// override executeImplDryRun to prevent calling getMapKeys multi times under
    /// same txn id, which may cause worker create table multi times
    ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, Array{})->convertToFullColumnIfConst();
    }

private:
    ContextPtr context;
};


class FunctionStrToMap: public IFunction
{
public:
    static constexpr auto name = "str_to_map";

    static FunctionPtr create(const ContextPtr &) { return std::make_shared<FunctionStrToMap>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 3; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    /// throw error when argument[0] column is constant value NULL
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 3 argument. Passed {}.", getName(), arguments.size());

        /// non-constant null values will be converted to String('') and returned Map({})
        auto arguments_without_nullable = createBlockWithNestedColumns(arguments);

        if (!isString(arguments_without_nullable[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be String, but parsed {}.", getName(), arguments_without_nullable[0].type->getName());

        if (!isString(arguments_without_nullable[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} (delimiter) must be String.", getName());

        if (!isString(arguments_without_nullable[2].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Third argument for function {} (delimiter) must be String.", getName());

        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnPtr column_prt = arguments[0].column;
        auto item_delimiter = getDelimiter(arguments[1].column);
        auto key_value_delimiter = getDelimiter(arguments[2].column);
        auto col_res = result_type->createColumn();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Map map;
            const StringRef & str = column_prt->getDataAt(i);
            const char * curr = str.data;
            const char * end = str.data + str.size;

            while (curr != end)
            {
                auto parsed_key = parseStringValue(curr, end, key_value_delimiter);

                /// skip space
                while (curr != end && *curr == ' ')
                    ++curr;

                /// will parse empty value if curr == end
                auto parsed_value = parseStringValue(curr, end, item_delimiter);

                /// skip space
                while (curr != end && *curr == ' ')
                    ++curr;

                map.emplace_back(parsed_key, parsed_value);
            }
            col_res->insert(map);
        }
        return col_res;
    }

private:

    static String parseStringValue(const char *& curr, const char * end, char delimiter)
    {
        const auto * begin = curr;
        size_t length = 0;
        while (curr != end && *curr != delimiter)
        {
            ++curr;
            ++length;
        }

        /// skip delimiter
        if (curr != end && *curr == delimiter)
            ++curr;

        return {begin, length};
    }

    inline char getDelimiter(const ColumnPtr & column_ptr) const
    {
        const auto & value = column_ptr->getDataAt(0);
        if (!value.size)
            throw Exception("Delimiter of function " + getName() + " should be non-empty string", ErrorCodes::ILLEGAL_COLUMN);
        return value.data[0];
    }
};

struct NameExtractMapColumn
{
    static constexpr auto name = "extractMapColumn";
};

struct NameExtractMapKey
{
    static constexpr auto name = "extractMapKey";
};

template <class Extract>
struct ExtractMapWrapper
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        size_t offsets_size = offsets.size();
        res_offsets.resize(offsets_size);

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets_size; ++i)
        {
            const char * src_data = reinterpret_cast<const char *>(&data[prev_offset]);
            size_t src_size = offsets[i] - prev_offset;
            auto res_view = Extract::apply(std::string_view(src_data, src_size));
            memcpy(reinterpret_cast<char *>(res_data.data() + res_offset), res_view.data(), res_view.size());

            res_offset += res_view.size() + 1; /// remember add 1 for null char
            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }

        res_data.resize(res_offset);
    }

    static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Column of type FixedString is not supported by extractMapColumn", ErrorCodes::ILLEGAL_COLUMN);
    }
};

REGISTER_FUNCTION(Map)
{
    factory.registerFunction<FunctionMap>();
    factory.registerFunction<FunctionMapContains>();
    factory.registerFunction<FunctionMapKeys>();
    factory.registerFunction<FunctionMapValues>();

    factory.registerFunction<FunctionMapElement>();
    factory.registerFunction<FunctionGetMapKeys>();
    factory.registerFunction<FunctionStrToMap>(FunctionFactory::CaseInsensitive);

    using FunctionExtractMapColumn = FunctionStringToString<ExtractMapWrapper<ExtractMapColumn>, NameExtractMapColumn>;
    using FunctionExtractMapKey = FunctionStringToString<ExtractMapWrapper<ExtractMapKey>, NameExtractMapKey>;
    factory.registerFunction<FunctionExtractMapKey>();
    factory.registerFunction<FunctionExtractMapColumn>();

    factory.registerFunction<FunctionMapContainsKeyLike>();
    factory.registerFunction<FunctionExtractKeyLike>();
    factory.registerFunction<FunctionMapUpdate>();
    factory.registerFunction<FunctionMapFromArrays>();

    factory.registerAlias("MAP_FROM_ARRAYS", "mapFromArrays");
    factory.registerAlias("map_keys", "mapKeys");
    factory.registerAlias("map_values", "mapValues");
}

}
