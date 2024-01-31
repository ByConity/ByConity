#include <iostream>
#include <string_view>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionSplitToMap : public IFunction
{
public:
    static constexpr auto name = "split_to_map";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSplitToMap>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 3",
                getName(),
                arguments.size());

        for (size_t i = 0; i < arguments.size(); i++)
        {
            DataTypePtr arg = arguments[i];
            if (arg->getTypeId() == TypeIndex::Nullable)
            {
                arg = typeid_cast<const DataTypeNullable *>(arg.get())->getNestedType();
            }
            if (!isStringOrFixedString(arg) && !isNothing(arg))
            {
                throw Exception{
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(i) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }
        }

        DataTypes keys, values;
        keys.emplace_back(std::make_shared<DataTypeString>());
        values.emplace_back(std::make_shared<DataTypeString>());
        DataTypes tmp;
        tmp.emplace_back(getLeastSupertype(keys));
        tmp.emplace_back(getLeastSupertype(values));
        return std::make_shared<DataTypeMap>(tmp);
    }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
        {
            return result_type->createColumn();
        }
        
        const auto & col_str = arguments[0].column;
        const auto & col_entry_delim = arguments[1].column;
        const auto & col_kv_delim = arguments[2].column;

        auto keys_data = ColumnString::create();
        auto values_data = ColumnString::create();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        keys_data->reserve(input_rows_count);
        values_data->reserve(input_rows_count);
        offsets->reserve(input_rows_count);
        IColumn::Offset current_offset = 0;

        size_t arg_size = arguments.size();

        // extract data columns and null maps if exist
        const UInt8 * nullable_args_map[arg_size];
        ColumnPtr raw_column_maps[arg_size];
        if (!extractNullMapAndNestedCol(arguments, raw_column_maps, nullable_args_map))
        {
            if (input_rows_count > 0)
            {
                offsets->insert(current_offset);
                auto nested_column
                = ColumnArray::create(ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}), std::move(offsets));

                return ColumnMap::create(nested_column);
            }   
        }
        
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (unlikely(checkNulls(nullable_args_map, row)))
            {
                keys_data->insertDefault();
                values_data->insertDefault();
                current_offset += 1;
                offsets->insert(current_offset);
                continue;
            }

            const auto & input_str = std::string_view(col_str->getDataAt(row));
            const auto & entry_delim = std::string_view(col_entry_delim->getDataAt(row));
            const auto & kv_delim = std::string_view(col_kv_delim->getDataAt(row));

            std::vector<std::string_view> entries;
            split(input_str, entry_delim, entries);

            for (const auto & entry : entries)
            {
                std::vector<std::string_view> kv_pair;
                split(entry, kv_delim, kv_pair);

                switch (kv_pair.size())
                {
                    case 1:
                        keys_data->insertData(kv_pair[0].data(), kv_pair[0].size());
                        values_data->insertDefault();
                        current_offset += 1;
                        break;
                    case 2:
                        keys_data->insertData(kv_pair[0].data(), kv_pair[0].size());
                        values_data->insertData(kv_pair[1].data(), kv_pair[1].size());
                        current_offset += 1;
                        break;
                    default:
                        keys_data->insertDefault();
                        values_data->insertDefault();
                        current_offset += 1;
                        break;
                }
            }
            offsets->insert(current_offset);
        }

        auto nested_column
            = ColumnArray::create(ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}), std::move(offsets));

        return ColumnMap::create(nested_column);
    }


private:
    inline bool checkNulls(const UInt8 ** nullable_args_map, size_t row_num) const
    {
        return (nullable_args_map[0] && nullable_args_map[0][row_num]) || (nullable_args_map[1] && nullable_args_map[1][row_num])
            || (nullable_args_map[2] && nullable_args_map[2][row_num]);
    }

    inline void split(const std::string_view & input, const std::string_view & delimiter, std::vector<std::string_view> & result) const
    {
        size_t current_position = 0;
        size_t input_size = input.size();
        size_t delimiter_size = delimiter.size();

        if (delimiter_size == 0)
            return;

        while (current_position < input_size)
        {
            size_t delimiter_position = input.find(delimiter, current_position);
            if (delimiter_position == String::npos)
            {
                result.emplace_back(input.data() + current_position, input_size - current_position);
                break;
            }
            result.emplace_back(input.data() + current_position, delimiter_position - current_position);
            current_position = delimiter_position + delimiter_size;
        }
        
    }
};

REGISTER_FUNCTION(FunctionSplitToMap)
{
    factory.registerFunction<FunctionSplitToMap>(FunctionFactory::CaseInsensitive);
}

}
