#include <Columns/ColumnString.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunctionMySql.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    class FunctionStrcmp : public IFunction
    {
    public:
        static constexpr auto name = "strcmp";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionStrcmp>(context->getSettingsRef().dialect_type == DialectType::MYSQL); }

        explicit FunctionStrcmp(bool mysql_mode_) : mysql_mode(mysql_mode_) {}

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 2; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (mysql_mode) return std::make_shared<DataTypeInt8>();

            if (!isString(arguments[0]) && !isFixedString(arguments[0]) && !isNumber(arguments[0]))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type of arguments ({}) for function {}",
                    arguments[0]->getName(),
                    getName());
            }

            if (!isString(arguments[1]) && !isFixedString(arguments[1]) && !isNumber(arguments[1]))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type of arguments ({}) for function {}",
                    arguments[1]->getName(),
                    getName());
            }

            return std::make_shared<DataTypeInt8>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            const auto & column1 = arguments[0].column->convertToFullColumnIfConst();
            const auto & column2 = arguments[1].column->convertToFullColumnIfConst();

            auto col1_serialized = ColumnString::create();
            auto col2_serialized = ColumnString::create();

            const IColumn * col1_data_ptr = column1.get();
            const IColumn * col2_data_ptr = column2.get();
            if (isColumnConst(*column1))
            {
                const ColumnConst * col1_const = assert_cast<const ColumnConst *>(col1_data_ptr);
                col1_data_ptr = &col1_const->getDataColumn();
            }

            if (isColumnConst(*column2))
            {
                const ColumnConst * col2_const = assert_cast<const ColumnConst *>(col2_data_ptr);
                col2_data_ptr = &col2_const->getDataColumn();
            }

            serializeColumn(col1_serialized, *col1_data_ptr, *arguments[0].type, input_rows_count);
            serializeColumn(col2_serialized, *col2_data_ptr, *arguments[1].type, input_rows_count);

            auto res = ColumnInt8::create();
            auto & res_data = res->getData();

            res_data.resize(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto & lhs = col1_serialized->getDataAt(i);
                const auto & rhs = col2_serialized->getDataAt(i);
                res_data[i] = strcmpImpl(lhs, rhs);
            }
            return res;
        }

    private:
        bool mysql_mode;
        inline void serializeColumn(
            COWHelper<DB::IColumn, DB::ColumnString>::MutablePtr & col_serialized,
            const IColumn & col_from,
            const IDataType & type,
            size_t input_rows_count) const
        {
            auto & col_text_offset = col_serialized->getOffsets();
            auto & col_text_data = col_serialized->getChars();
            col_text_data.resize(input_rows_count);
            col_text_offset.resize(input_rows_count);

            WriteBufferFromVector<ColumnString::Chars> write_buffer(col_text_data);
            FormatSettings format_settings;
            auto serialization = type.getDefaultSerialization();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                serialization->serializeText(col_from, i, write_buffer, format_settings);
                writeChar('\0', write_buffer);
                col_text_offset[i] = write_buffer.count() + 1;
            }
        }

        inline int strcmpImpl(const StringRef & lhs, const StringRef & rhs) const
        {
            size_t min_size = std::min(lhs.size, rhs.size);
            for (size_t i = 0; i < min_size; ++i)
            {
                int cmp = static_cast<unsigned char>(lhs.data[i]) - static_cast<unsigned char>(rhs.data[i]);
                if (cmp != 0)
                {
                    return cmp < 0 ? -1 : 1;
                }
            }
            if (lhs.size == rhs.size)
            {
                return 0;
            }
            return lhs.size < rhs.size ? -1 : 1;
        }
    };

}

REGISTER_FUNCTION(Strcmp)
{
    factory.registerFunction<FunctionStrcmp>("strcmp", FunctionFactory::CaseInsensitive);
}

}
