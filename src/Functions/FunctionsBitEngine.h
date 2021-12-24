#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnBitMap64.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeBitMap64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionBitEngineHelpers.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionArrayToBitmapWithEncode : public IFunction
{
public:
    static constexpr auto name = "arrayToBitmapWithEncode";

    static FunctionPtr create(const ContextPtr & context_) { return std::make_shared<FunctionArrayToBitmapWithEncode>(context_); }

    explicit FunctionArrayToBitmapWithEncode(const ContextPtr & context_) : context(context_) { }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 4 && arguments.size() != 5)
            throw Exception{"Function " + getName() + " requires four or five arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        bool ok = checkAndGetDataType<DataTypeString>(arguments[1].get())
            && checkAndGetDataType<DataTypeString>(arguments[2].get())
            && checkAndGetDataType<DataTypeString>(arguments[3].get());
        if (arguments.size() == 5)
            ok = ok && checkAndGetDataType<DataTypeUInt8>(arguments[4].get());

        if (!ok)
            throw Exception("Illegal data type of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments[0]->onlyNull())
            return arguments[0];

        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception(
                "First argument for function " + getName() + " must be an array but it has type " + arguments[3]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeBitMap64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /* input_rows_count */) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * array_type = checkAndGetDataType<DataTypeArray>(from_type);
        const auto & nested_type = array_type->getNestedType();

        DataTypes argument_types = {nested_type};

        WhichDataType which(nested_type);
        if (which.isUInt8())
            return executeIntType<UInt8>(arguments);
        else if (which.isUInt16())
            return executeIntType<UInt16>(arguments);
        else if (which.isUInt32())
            return executeIntType<UInt32>(arguments);
        else if (which.isUInt64())
            return executeIntType<UInt64>(arguments);
        else if (which.isInt8())
            return executeIntType<Int8>(arguments);
        else if (which.isInt16())
            return executeIntType<Int16>(arguments);
        else if (which.isInt32())
            return executeIntType<Int32>(arguments);
        else if (which.isInt64())
            return executeIntType<Int64>(arguments);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    ContextPtr context;

    template <typename T>
    ColumnPtr executeIntType(const ColumnsWithTypeAndName & arguments) const
    {
        const IColumn * column_first = arguments[0].column.get();
        const auto * column_array = checkAndGetColumnEvenIfConst<ColumnArray>(column_first);
        if (!column_array)
            throw Exception("Function " + getName() + " has illegal first argument where an UInt array is required",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        String database, table, dict_name;
        std::tie(database, table, dict_name) = getDictionaryPrerequisite(arguments, getName());

        bool add_new_id_to_bitengine_dict = false;
        if (arguments.size() == 5)
        {
            const auto * setting_column = checkAndGetColumnEvenIfConst<ColumnUInt8>(arguments[4].column.get());
            add_new_id_to_bitengine_dict = setting_column->getBool(0);
        }

        const ColumnPtr & mapped = column_array->getDataPtr();
        const ColumnArray::Offsets & offsets = column_array->getOffsets();
        const ColumnVector<T> * column = checkAndGetColumn<ColumnVector<T>>(&*mapped);
        const typename ColumnVector<T>::Container & input_data = column->getData();

        auto col_to = ColumnBitMap64::create();
        col_to->reserve(offsets.size());

        size_t pos = 0;
        for (auto offset : offsets)
        {
            PODArray<UInt64> input;

            for ( ; pos < offset; ++pos)
            {
                input.emplace_back(static_cast<UInt64>(input_data[pos]));
            }
            if (input.empty())
                col_to->insertDefault();
            else
                col_to->insert(BitMap64(input.size(), input.data()));
        }

        ColumnWithTypeAndName column_bitmap(std::move(col_to), std::make_shared<DataTypeBitMap64>(), "btimap_encoded");

        ColumnPtr column_res = add_new_id_to_bitengine_dict
            ? encodeColumnAddUnknown(column_bitmap, database, table, dict_name, context)
            : encodeColumnDiscardUnknown(column_bitmap, database, table, dict_name, context); // arrayToBitmapWithEncode

       return column_res;
    }
};

class FunctionEncodeNonBitEngineColumn : public IFunction
{
public:
    static constexpr auto name = "EncodeNonBitEngineColumn";

    static FunctionPtr create(const ContextPtr & context) { return std::make_shared<FunctionEncodeNonBitEngineColumn>(context); }

    explicit FunctionEncodeNonBitEngineColumn(const ContextPtr & context_) : context(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 4 && arguments.size() != 5)
            throw Exception{"Function " + getName() + " requires four or five arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        // auto type = checkAndGetDataType<DataTypeUInt64>(arguments[0].get());
        if (!checkDataTypeForBitEngineEncode(arguments[0]))
            throw Exception("Function " + getName() + " should has under 64-bit column for its first argument",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        bool ok = checkAndGetDataType<DataTypeString>(arguments[1].get())
            && checkAndGetDataType<DataTypeString>(arguments[2].get())
            && checkAndGetDataType<DataTypeString>(arguments[3].get());
        if (arguments.size() == 5)
            ok = ok && checkAndGetDataType<DataTypeUInt8>(arguments[4].get());

        if (!ok)
            throw Exception("Illegal data type of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr int_column = arguments[0].column;
        bool is_column_const = isColumnConst(*int_column);
        if (is_column_const)
            int_column = checkAndGetColumnConstWithoutCheck(int_column.get())->getDataColumnPtr();

        if (!int_column)
            throw Exception("Function " + getName() + " has illegal first argument where an NativeInteger is required",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        String database, table, dict_name;
        std::tie(database, table, dict_name) = getDictionaryPrerequisite(arguments, getName());

        bool add_new_id_to_bitengine_dict = false;
        if (arguments.size() == 5)
        {
            const auto * setting_column = checkAndGetColumnEvenIfConst<ColumnUInt8>(arguments[4].column.get());
            add_new_id_to_bitengine_dict = setting_column->getBool(0);
        }

        ColumnWithTypeAndName column = arguments[0];
        if (is_column_const)
            column.column = int_column;

        ColumnPtr column_res = add_new_id_to_bitengine_dict
            ? encodeColumnAddUnknown(column, database, table, dict_name, context)
            : encodeColumnDiscardUnknown(column, database, table, dict_name, context);   // EncodeNonBitEngineColumn

        return is_column_const ? ColumnConst::create(column_res, input_rows_count) : column_res;
    }

private:
    ContextPtr context;
};

class FunctionEncodeBitmap : public IFunction
{
public:
    static constexpr auto name = "EncodeBitmap";

    static FunctionPtr create(const ContextPtr & context) { return std::make_shared<FunctionEncodeBitmap>(context); }

    explicit FunctionEncodeBitmap(const ContextPtr & context_) : context(context_) { }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 4 && arguments.size() != 5)
            throw Exception{"Function " + getName() + " requires four or five arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto *bitmap_type = checkAndGetDataType<DataTypeBitMap64>(arguments[0].get());
        if (!bitmap_type)
            throw Exception{"Function " + getName() + " should has BitMap64 column for its first arguemnt", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        bool ok = checkAndGetDataType<DataTypeString>(arguments[1].get())
            && checkAndGetDataType<DataTypeString>(arguments[2].get())
            && checkAndGetDataType<DataTypeString>(arguments[3].get());
        if (arguments.size() == 5)
            ok = ok && checkAndGetDataType<DataTypeUInt8>(arguments[4].get());

        if (!ok)
            throw Exception("Illegal data type of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeBitMap64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr bitmap_column = arguments[0].column;
        bool is_column_const = isColumnConst(*bitmap_column);
        if (is_column_const)
            bitmap_column = checkAndGetColumnConst<ColumnBitMap64>(bitmap_column.get())->getDataColumnPtr();
        if (!bitmap_column)
            throw Exception("Function " + getName() + " has illegal first argument where a BitMap64 is required",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        String database, table, dict_name;
        std::tie(database, table, dict_name) = getDictionaryPrerequisite(arguments, getName());
        bool add_new_id_to_bitengine_dict = false;
        if (arguments.size() == 5)
        {
            const auto * setting_column = checkAndGetColumnEvenIfConst<ColumnUInt8>(arguments[4].column.get());
            add_new_id_to_bitengine_dict = setting_column->getBool(0);
        }

        ColumnWithTypeAndName column = arguments[0];
        if (is_column_const)
            column.column = bitmap_column;

        ColumnPtr column_res = add_new_id_to_bitengine_dict
            ? encodeColumnAddUnknown(column, database, table, dict_name, context)
            : encodeColumnDiscardUnknown(column, database, table, dict_name, context); // EncodeBitmap

        return is_column_const ? ColumnConst::create(column_res, input_rows_count) : column_res;
    }

private:
    ContextPtr context;
};



class FunctionBitmapToArrayWithDecode : public IFunction
{
public:
    static constexpr auto name = "bitmapToArrayWithDecode";

    static FunctionPtr create(const ContextPtr & context_) { return std::make_shared<FunctionBitmapToArrayWithDecode>(context_); }

    explicit FunctionBitmapToArrayWithDecode(const ContextPtr & context_) : context(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 4; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * bitmap64_type = checkAndGetDataType<DataTypeBitMap64>(arguments[0].get());

        if (!bitmap64_type)
            throw Exception("Function " + getName() + " requires BitMap64 as its first argument.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        bool ok = checkAndGetDataType<DataTypeString>(arguments[1].get())
            && checkAndGetDataType<DataTypeString>(arguments[2].get())
            && checkAndGetDataType<DataTypeString>(arguments[3].get());

        if (!ok)
            throw Exception("Illegal data type of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        // First, decode the bitmap
        ColumnPtr bitmap_column = arguments[0].column;
        bool is_column_const = isColumnConst(*bitmap_column);
        if (is_column_const)
            bitmap_column = checkAndGetColumnConst<ColumnBitMap64>(bitmap_column.get())->getDataColumnPtr();
        if (!bitmap_column)
            throw Exception("Function " + getName() + " has illegal first argument where a BitMap64 is required",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        String database, table, dict_name;
        std::tie(database, table, dict_name) = getDictionaryPrerequisite(arguments, getName());

        ColumnWithTypeAndName column = arguments[0];
        if (is_column_const)
            column.column = bitmap_column;

        // bitmapToArrayWithDecode
        auto column_decoded = decodeColumn(column, database, table, dict_name, false, context);

        // convert the decoded bitmap to array
        const auto & return_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
        auto res_ptr = return_type->createColumn();
        ColumnArray & res = dynamic_cast<ColumnArray &>(*res_ptr);

        IColumn & res_data = res.getData();
        ColumnArray::Offsets & res_offsets = res.getOffsets();

        executeBitmapType(const_cast<IColumn *>(column_decoded.get()), res_data, res_offsets, input_rows_count);

        return res_ptr;
    }

private:
    void executeBitmapType(IColumn * input_data_col,
                           IColumn & res_data_col, ColumnArray::Offsets & res_offsets, size_t input_rows_count) const
    {
        const auto * res_column = checkAndGetColumn<ColumnUInt64>(&res_data_col);
        if (!res_column)
            throw Exception("BitMap64 can be only converted to UInt64", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        ColumnUInt64::Container & res_container = const_cast<ColumnUInt64 *>(res_column)->getData();
        ColumnArray::Offset res_offset = 0;

        const auto * column_bitmap = checkAndGetColumn<ColumnBitMap64>(input_data_col);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const BitMap64 & bitmap_64 = column_bitmap->getBitMapAt(i);
            size_t size = bitmap_64.cardinality();

            for (auto it : bitmap_64)
            {
                res_container.emplace_back(it);
            }
            res_offset += size;

            res_offsets.emplace_back(res_offset);
        }
    }

    ContextPtr context;
};

class FunctionDecodeNonBitEngineColumn : public IFunction
{
public:
    static constexpr auto name = "DecodeNonBitEngineColumn";

    static FunctionPtr create(const ContextPtr & context) { return std::make_shared<FunctionDecodeNonBitEngineColumn>(context); }

    explicit FunctionDecodeNonBitEngineColumn(const ContextPtr & context_) : context(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 4; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 4)
            throw Exception{"Function " + getName() + " requires four arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        if (!checkDataTypeForBitEngineDecode(arguments[0]))
            throw Exception{"Function " + getName() + " should has under-64-bit unsigned Intger column for its first arguemnt",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        bool ok = checkAndGetDataType<DataTypeString>(arguments[1].get())
            && checkAndGetDataType<DataTypeString>(arguments[2].get())
            && checkAndGetDataType<DataTypeString>(arguments[3].get());

        if (!ok)
            throw Exception("Illegal data type of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr int_column = arguments[0].column;
        bool is_column_const = isColumnConst(*int_column);

        if (is_column_const)
            int_column = checkAndGetColumnConstWithoutCheck(int_column.get())->getDataColumnPtr();

        if (!int_column)
            throw Exception("Function " + getName() + " has illegal Data Column for encode",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        String database, table, dict_name;
        std::tie(database, table, dict_name) = getDictionaryPrerequisite(arguments, getName());

        ColumnWithTypeAndName column = arguments[0];
        if (is_column_const)
            column.column = int_column;

        auto column_res = decodeColumn(column, database, table, dict_name, false, context);  // DecodeNonBitEngineColumnv2

        return is_column_const
            ? ColumnConst::create(column_res, input_rows_count)
            : column_res;
    }

private:
    ContextPtr context;
};

class FunctionDecodeBitmap : public IFunction
{
public:
    static constexpr auto name = "DecodeBitmap";

    static FunctionPtr create(const ContextPtr & context_) { return std::make_shared<FunctionDecodeBitmap>(context_); }

    explicit FunctionDecodeBitmap(const ContextPtr & context_) : context(context_) { }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 4; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 4)
            throw Exception{"Function " + getName() + " requires four arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto *bitmap_type = checkAndGetDataType<DataTypeBitMap64>(arguments[0].get());
        if (!bitmap_type)
            throw Exception{"Function " + getName() + " should has BitMap64 column for its first arguemnt", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        bool ok = checkAndGetDataType<DataTypeString>(arguments[1].get())
            && checkAndGetDataType<DataTypeString>(arguments[2].get())
            && checkAndGetDataType<DataTypeString>(arguments[3].get());

        if (!ok)
            throw Exception("Illegal data type of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeBitMap64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr bitmap_column = arguments[0].column;
        bool is_column_const = isColumnConst(*bitmap_column);
        if (is_column_const)
            bitmap_column = checkAndGetColumnConst<ColumnBitMap64>(bitmap_column.get())->getDataColumnPtr();
        if (!bitmap_column)
            throw Exception("Function " + getName() + " has illegal first argument where a BitMap64 is required",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        String database, table, dict_name;
        std::tie(database, table, dict_name) = getDictionaryPrerequisite(arguments, getName());

        ColumnWithTypeAndName column = arguments[0];
        if (is_column_const)
            column.column = bitmap_column;

        auto column_res = decodeColumn(column, database, table, dict_name, false, context);  // DecodeBitmap

        return is_column_const
            ? ColumnConst::create(column_res, input_rows_count)
            : column_res;
    }

private:
    ContextPtr context;
};

class FunctionBitEngineDecode : public IFunction
{
public:
    static constexpr auto name = "BitEngineDecode";

    static FunctionPtr create(const ContextPtr & context) { return std::make_shared<FunctionBitEngineDecode>(context); }

    explicit FunctionBitEngineDecode(const ContextPtr & context_) : context(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception{"Function " + getName() + " requires three arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto *bitmap_type = checkAndGetDataType<DataTypeBitMap64>(arguments[0].get());
        if (!bitmap_type)
            throw Exception{"Function " + getName() + " should has BitMap64 column for its first argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        bool ok = checkAndGetDataType<DataTypeString>(arguments[1].get()) && checkAndGetDataType<DataTypeString>(arguments[2].get());

        if (!ok)
            throw Exception("Illegal data type of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeBitMap64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * bitmap_column =
            checkAndGetColumnEvenIfConst<ColumnBitMap64>(arguments[0].column.get());
        if (!bitmap_column)
            throw Exception("Function " + getName() + " has illegal first argument where a BitMap64 is required",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        String database, table;
        std::tie(database, table, std::ignore) = getDictionaryPrerequisite(arguments, getName());

        ColumnWithTypeAndName column = arguments[0];
        return decodeColumn(column, database, table, column.name, true, context); //BitEngineDecode
    }

private:
    ContextPtr context;
};

}


