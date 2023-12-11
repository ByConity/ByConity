#include <cstddef>
#include <memory>
#include <utility>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/Context_fwd.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Common/Exception.h>
#include <Common/ChineseTokenExtractor.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


class FunctionChineseTokenExtractor : public IFunction
{
public:

    static constexpr auto name =  "chineseTokens";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionChineseTokenExtractor>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2;}
    bool isVariadic() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return ColumnNumbers{1} ; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires 2 arguments", getName());
        }

        auto input_type = WhichDataType(arguments[0].type);
        auto input_config_type = WhichDataType(arguments[1].type);

        if (!input_type.isStringOrFixedString())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {} first argument type should be String or FixedString, but with {}",
                getName(),
                arguments[0].type->getName());

        if (!input_config_type.isString())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {} second argument type should be String, but with {}",
                getName(),
                arguments[1].type->getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());        
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        auto column_offsets = ColumnArray::ColumnOffsets::create();
        
        Field config_argument_value;
        arguments[1].column->get(0, config_argument_value);
        String tokenizer_config_name = config_argument_value.safeGet<String>();

        ChineseTokenExtractor extractor(tokenizer_config_name);

        auto result_column_string = ColumnString::create();

        auto input_column = arguments[0].column;

        size_t current_tokens_size = 0 ;
        size_t column_size = input_column->size();

        auto & offsets_data = column_offsets->getData();
        offsets_data.resize(column_size);

        ChineseTokenExtractor::WordRangesWithIterator iterator;

        for (size_t i = 0; i < column_size; ++i)
        {
            
            auto data = input_column->getDataAt(i);

            extractor.preCutString(data.toString(),iterator);

            size_t token_start = 0;
            size_t token_length = 0;

            while (ChineseTokenExtractor::nextInCutString(token_start, token_length, iterator))
            {
                result_column_string->insertData(data.data + token_start, token_length);
                ++current_tokens_size;
            }
            offsets_data[i] = current_tokens_size;

            ChineseTokenExtractor::clearForNextCut(iterator);
        }

        return ColumnArray::create(std::move(result_column_string), std::move(column_offsets));
    }

};

REGISTER_FUNCTION(ChineseStringTokenExtractor)
{
    factory.registerFunction<FunctionChineseTokenExtractor>();
}

}
