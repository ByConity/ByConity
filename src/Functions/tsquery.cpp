
#include <Common/config.h>

#if USE_TSQUERY

#include <algorithm>
#include <cstddef>
#include <memory>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include "Common/Exception.h"
#include "Common/assert_cast.h"
#include "common/types.h"
#include <Common/TextSreachQuery.h>
#include "Columns/ColumnArray.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnVector.h"
#include "Columns/IColumn.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "Core/DecimalFunctions.h"
#include "DataTypes/DataTypeArray.h"
#include "DataTypes/DataTypeFixedString.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/IDataType.h"
#include "Interpreters/Context_fwd.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

class FunctionTextSearchQuery : public IFunction
{
public:
    static constexpr auto name = "textSearch";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionTextSearchQuery>();
    }

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes&) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    // do noting here, use row id filter with gin index.
    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<UInt8>::create();
        PaddedPODArray<UInt8> & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);
        std::fill(vec_res.begin(), vec_res.end(), 1);
        return col_res;
    }
};


class ToTextSearchQuery : public IFunction
{
public:
    static constexpr auto name = "toTextSearchQuery";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<ToTextSearchQuery>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1;}

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires 1 argument", getName());
        }

        auto input_type = WhichDataType(arguments[0].type);

        if (!input_type.isStringOrFixedString())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {} argument type should be String or FixedString, but with {}",
                getName(),
                arguments[0].type->getName());
        }
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        auto result_column_string = ColumnString::create();

        auto input_column = arguments[0].column;
        size_t column_size = input_column->size();

        for (size_t i = 0; i < column_size; ++i)
        {
            auto data = input_column->getDataAt(i);
            TextSearchQuery query(data.toString());

            String ast_str = query.toString();
            result_column_string->insertData(ast_str.data(), ast_str.length());
        }

        return result_column_string;
    } 
};

REGISTER_FUNCTION(TSQuery)
{
    factory.registerFunction<FunctionTextSearchQuery>();
    factory.registerFunction<ToTextSearchQuery>();
}


}

#endif
