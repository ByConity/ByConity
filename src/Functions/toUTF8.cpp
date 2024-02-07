#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionMySql.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
}


class FunctionToUTF8 : public IFunction
{
public:
    static FunctionPtr create(ContextPtr context)
    {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return std::make_shared<IFunctionMySql>(std::make_unique<FunctionToUTF8>());
        return std::make_shared<FunctionToUTF8>();
    }

    ArgType getArgumentsType() const override { return ArgType::STRINGS; }

    String getName() const override { return "TO_UTF8"; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong number of arguments for function {}: 1 expected.", getName());

        if (!WhichDataType(arguments[0]).isStringOrFixedString())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Must be FixedString or String.",
                arguments[0]->getName(),
                getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & , size_t input_rows_count) const override
    {
        const ColumnPtr & src_column = arguments[0].column;
        auto dst_column = ColumnString::create();

        auto process_string = [&](const StringRef & str, size_t size) {
            static const char * hex_chars = "0123456789ABCDEF";
            char output[size * 2 + 2];

            output[0] = '0';
            output[1] = 'x';
            for (size_t i = 0; i < size; ++i)
            {
                output[i * 2 + 2] = hex_chars[static_cast<unsigned char>(str.data[i]) >> 4];
                output[i * 2 + 3] = hex_chars[static_cast<unsigned char>(str.data[i]) & 0xF];
            }
            dst_column->insertData(output, size * 2 + 2);
        };

        if (const auto * src_column_string = typeid_cast<const ColumnString *>(src_column.get()))
        {
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                const auto str_ref = src_column_string->getDataAt(row);
                process_string(str_ref, str_ref.size);
            }
        }
        else if (const auto * src_column_fixed_string = typeid_cast<const ColumnFixedString *>(src_column.get()))
        {
            size_t n = src_column_fixed_string->getN();
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                const auto str_ref = src_column_fixed_string->getDataAt(row);
                process_string(str_ref, n);
            }
        }
        else
        {
            throw Exception("Unsupported column type for FunctionToUTF8", ErrorCodes::ILLEGAL_COLUMN);
        }

        return dst_column;
    }
};

REGISTER_FUNCTION(ToUTF8)
{
    factory.registerFunction<FunctionToUTF8>("TO_UTF8", FunctionFactory::CaseInsensitive);
}

}
