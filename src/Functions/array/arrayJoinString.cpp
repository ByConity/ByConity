#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/memcmpSmall.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/castColumn.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TYPE_MISMATCH;
}

using NullMap = PaddedPODArray<UInt8>;

/// for each array, join its elements with the second argument as the separator.
/// e.g., array_join([1,2,3], 'x') -> '1x2x3'
/// array_join([1,NULL, 2], 'x') -> '1x2'
/// array_join([1,NULL, 2], NULL) -> NULL
/// implicit type conversion to string for the args is enabled
class FunctionArrayJoinString : public IFunction
{
public:
    static constexpr auto name = "array_join";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayJoinString>(); }
    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    /// first arg is Array(T) or Nullable(Array(T))
    /// second arg is Nullable(T) or T
    /// if any arg is Nullable, the return type is Nullable(String); otherwise String.
    /// note: the above T could be a basic type like String, Date or a composite type
    /// LC(T'), Nullable(T') or LC(Nullable(T')) where T' is a basic type
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(arguments[0].get());
        if (nullable_type)
            array_type = checkAndGetDataType<DataTypeArray>(nullable_type->getNestedType().get());

        if (!array_type)
            throw Exception("First argument for function " + getName() + " must be an array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->onlyNull() && !allowArguments(array_type->getNestedType(), arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Types of array and 2nd argument of function `{}` must be number, string, date or date time types. "
                "Passed: {} and {}.",
                getName(), arguments[0]->getName(), arguments[1]->getName());


        if (nullable_type || checkDataTypes<DataTypeNullable>(removeLowCardinality(arguments[1]).get()))
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());

        return std::make_shared<DataTypeString>();
    }

    /// if the array column is nested in a nullable column,
    /// process the nested array column then wrap the result into a nullable column to return
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Execute on nested columns and wrap results with nullable
        const auto * nullable_col = checkAndGetColumn<ColumnNullable>(arguments[0].column.get());
        const auto * const_col = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
        if (const_col)
            nullable_col = checkAndGetColumn<ColumnNullable>(const_col->getDataColumn());

        if (nullable_col)
        {
            /// put the array column into const if the original col is const(nullable(arr))
            auto arr_col = const_col ? ColumnConst::create(nullable_col->getNestedColumnPtr(), const_col->size()) : nullable_col->getNestedColumnPtr();
            bool is_sep_nullable = isNullableOrLowCardinalityNullable(arguments[1].type);
            ColumnsWithTypeAndName tmp_args = {{arr_col, removeNullable(arguments[0].type), arguments[0].name}, arguments[1]};
            auto res = executeInternalImpl(tmp_args, is_sep_nullable ? result_type : removeNullable(result_type), input_rows_count);
            return wrapInNullable(res, arguments, result_type, input_rows_count);
        }

        return executeInternalImpl(arguments, result_type, input_rows_count);
    }

private:
    static inline bool allowArguments(const DataTypePtr & array_inner_type, const DataTypePtr & arg)
    {
        auto inner_type_decayed = removeNullable(removeLowCardinality(array_inner_type));
        auto sep_decayed = removeNullable(removeLowCardinality(arg));

        return ((isNothing(inner_type_decayed) || isNumberOrString(inner_type_decayed) || isDateOrDateTime(inner_type_decayed)) && (isNumberOrString(sep_decayed)
                || isDateOrDateTime(sep_decayed)));
    }

    static String processForOneRow(ColumnPtr data, const NullMap * null_map, size_t start, size_t size, const String & sep)
    {
        if (size == 0)
            return "";
        String current;
        bool joined = false;
        for (size_t i = start; i < start + size; ++i)
        {
            if (null_map && (*null_map)[i])
                continue;

            if (current.empty())
            {
                current = data->getDataAt(i).toString();
            }
            else
            {
                joined = true;
                /// TODO optimize for String and FixedString column by accessing
                /// the data via memory address instead of getDataAt()
                current = current + sep + data->getDataAt(i).toString();
            }
        }
        /// if there is fewer than 2 valid elements in the array, no join is not performed
        return joined ? current : "";
    }

    /// data is a string or fixedstring column flattened from columnarray
    /// offsets is like [3, 13, 20...] where 3 is the number of elements in the first (row) array;
    /// 13-3=10 is the number of elements in the second (row) array;
    static ColumnPtr processForAllRows(ColumnPtr data, const NullMap * null_map, const ColumnArray::Offsets & offsets,
            const String & sep, DataTypePtr result_type)
    {
        auto res = result_type->createColumn();

        size_t current_offset = 0;
        for (const auto offset : offsets)
        {
            const auto joined = processForOneRow(data, null_map, current_offset, offset - current_offset, sep);
            res->insertData(joined.data(), joined.length());
            current_offset = offset;
        }

        return res;
    }

    static ColumnPtr processForAllRows(ColumnPtr data, const NullMap * null_map_data, const ColumnArray::Offsets & offsets,
            ColumnPtr sep, const NullMap * null_map_sep, DataTypePtr result_type)
    {
        assert(sep->size() == offsets.size());
        auto res = result_type->createColumn();
        size_t current_offset = 0;
        for (size_t i = 0; i < offsets.size(); i++)
        {
            if (null_map_sep && (*null_map_sep)[i])
            {
                res->insertDefault();
            }
            else
            {
                const auto joined = processForOneRow(data, null_map_data, current_offset, offsets[i] - current_offset, sep->getDataAt(i).toString());
                res->insertData(joined.data(), joined.length());
                current_offset = offsets[i];
            }
        }

        return res;
    }

    static ColumnPtr executeInternalImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/)
    {
        auto ret = executeConst(arguments, result_type);
        if (!ret)
            ret = executeNonConst(arguments, result_type);
        return ret;
    }

    /// conver the second argument, i.e., the column for the separator, to string type if necessary
    /// the result sep_col may be Const and Nullable
    static ColumnPtr convertSeparatorColumn(const ColumnWithTypeAndName & arg, ColumnPtr & sep_col)
    {
        /// do conversion against the internal column if it is a const column
        auto non_const_col = arg.column;
        const auto * const_col = checkAndGetColumn<ColumnConst>(arg.column.get());
        if (const_col)
            non_const_col = const_col->getDataColumnPtr();

        if (!isStringOrFixedString(*removeNullable(removeLowCardinality(arg.type))))
        {
            DataTypePtr str_type = std::make_shared<DataTypeString>();
            if (isNullableOrLowCardinalityNullable(arg.type))
                str_type = std::make_shared<DataTypeNullable>(str_type);
            sep_col = castColumn(ColumnWithTypeAndName{non_const_col, arg.type, ""}, str_type);
        }
        else
        {
            sep_col = non_const_col->convertToFullColumnIfLowCardinality();
        }

        ColumnPtr sep_null_map = nullptr;
        if (const auto * sep_null_col = checkAndGetColumn<ColumnNullable>(sep_col.get()); sep_null_col)
        {
            sep_null_map = sep_null_col->getNullMapColumnPtr();
            /// must reset sep_col after get sep_null_map; otherwise sep_null_col points to invalid mem
            sep_col = sep_null_col->getNestedColumnPtr();
        }

        /// wrap it back into const column
        if (const_col)
            sep_col = ColumnConst::create(sep_col, const_col->size());

        return sep_null_map;
    }

    /// conver the first argument, i.e., the array, to string type if necessary
    /// the result str_col may be Nullable
    static ColumnPtr convertArrayColumn(const ColumnArray * arr_col, DataTypePtr type, ColumnPtr & str_col)
    {
        const auto * arr_type = checkAndGetDataType<DataTypeArray>(type.get());
        if(!arr_type)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "DataTypeArray is expected, but got {}", arr_type->getName());

        if (!isStringOrFixedString(*removeNullable(removeLowCardinality(arr_type->getNestedType()))))
        {
            DataTypePtr str_type = std::make_shared<DataTypeString>();
            if (isNullableOrLowCardinalityNullable(arr_type->getNestedType()))
                str_type = std::make_shared<DataTypeNullable>(str_type);
            str_col = castColumn({arr_col->getDataPtr(), arr_type->getNestedType(), ""}, str_type);
        }
        else
        {
            /// TODO optimize for LC by handle it explicity without converting to full column
            str_col =  arr_col->getDataPtr()->convertToFullColumnIfLowCardinality();
        }

        ColumnPtr str_null_map = nullptr;
        if (const auto * str_null_col = checkAndGetColumn<ColumnNullable>(str_col.get()); str_null_col)
        {
            str_null_map = str_null_col->getNullMapColumnPtr();
            /// must reset str_col after get str_null_map; otherwise str_null_col points to invalid mem
            str_col = str_null_col->getNestedColumnPtr();
        }
        return str_null_map;
    }

    /// the data coloum is Const(Array) or Const(Nullable(Array))
    static ColumnPtr executeConst(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type)
    {
        const ColumnConst * col_const = checkAndGetColumnConst<ColumnArray>(arguments[0].column.get());

        if (!col_const)
            return nullptr;

        if (arguments[1].type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(arguments[1].column->size());

        ColumnPtr sep_col = nullptr;
        const ColumnPtr sep_null_map = convertSeparatorColumn(arguments[1], sep_col);
        const NullMap * sep_null_map_raw = sep_null_map ? &(assert_cast<const ColumnUInt8 &>(*sep_null_map).getData()) : nullptr;

        const ColumnArray * arr_col = assert_cast<const ColumnArray *>(col_const->getDataColumnPtr().get());
        ColumnPtr str_col;
        const ColumnPtr str_null_map = convertArrayColumn(arr_col, arguments[0].type, str_col);
        const NullMap * str_null_map_raw = str_null_map ? &(assert_cast<const ColumnUInt8 &>(*str_null_map).getData()) : nullptr;

        if (isColumnConst(*sep_col))
        {
            String sep = sep_col->getDataAt(0).toString();
            const String current = processForOneRow(str_col, str_null_map_raw, 0, str_col->size(), sep);
            return result_type->createColumnConst(arr_col->size(), current);
        }
        else
        {
            auto res_col = result_type->createColumn();
            for (size_t row = 0; row < sep_col->size(); ++row)
            {
                if (sep_null_map && (*sep_null_map_raw)[row])
                {
                    res_col->insertDefault();
                    continue;
                }

                const auto & sep = sep_col->getDataAt(row).toString();
                res_col->insert(processForOneRow(str_col, str_null_map_raw, 0, str_col->size(), sep));
            }

            return res_col;
        }
    }

    static ColumnPtr executeNonConst(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type)
    {
        const ColumnArray * arr_col = assert_cast<const ColumnArray *>(arguments[0].column.get());

        if (arguments[1].type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(arr_col->size());

        ColumnPtr sep_col = nullptr;
        const ColumnPtr sep_null_map = convertSeparatorColumn(arguments[1], sep_col);
        const NullMap * sep_null_map_raw = sep_null_map ? &(assert_cast<const ColumnUInt8 &>(*sep_null_map).getData()) : nullptr;

        ColumnPtr str_col;
        const ColumnPtr str_null_map = convertArrayColumn(arr_col, arguments[0].type, str_col);
        const NullMap * str_null_map_raw = str_null_map ? &(assert_cast<const ColumnUInt8 &>(*str_null_map).getData()) : nullptr;
        if (isColumnConst(*sep_col))
        {
            String sep = sep_col->getDataAt(0).toString();
            return processForAllRows(str_col, str_null_map_raw, arr_col->getOffsets(), sep, result_type);
        }
        else
        {
            auto res_col = result_type->createColumn();
            return processForAllRows(str_col, str_null_map_raw, arr_col->getOffsets(), sep_col, sep_null_map_raw, result_type);
        }
    }

};


REGISTER_FUNCTION(ArrayJoinString)
{
    factory.registerFunction<FunctionArrayJoinString>();
}

}
