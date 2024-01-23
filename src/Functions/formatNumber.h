#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/FunctionsRound.h>
#include <Functions/IFunction.h>


namespace DB
{

struct FormatNumberImpl
{
    static ColumnPtr
    formatExecute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &/*result_type*/, size_t input_row_counts)
    {
        // TODO: add support for an optional locale argument
        const DataTypePtr & data_type = arguments[0].type;

        // round raw numbers
        auto func_builder_round = FunctionFactory::instance().get("round", nullptr);
        auto rounded_data_ptr = func_builder_round->build(arguments)->execute(arguments, data_type, input_row_counts);

        // convert to string
        ColumnsWithTypeAndName rounded_data_cols = {{rounded_data_ptr, data_type, "rounded"}};
        const ColumnPtr rounded_str_cols = ConvertImplGenericToString<ColumnString>::execute(rounded_data_cols, std::make_shared<DataTypeString>(), input_row_counts);
        size_t col_size = rounded_str_cols->size();
        auto res_col = ColumnString::create();

        int decimal_precision = getScaleArg(arguments);

        for (size_t id = 0; id < col_size; id++)
        {
            auto src_str = std::string_view(rounded_str_cols->getDataAt(id));

            size_t str_len = src_str.length();
            size_t whole_len = std::min(src_str.find('.'), str_len);
            size_t decimal_len = str_len - whole_len - 1;

            int fill_num = decimal_precision - decimal_len;
            // calculate comma number for locale
            size_t comma_num = (whole_len - (src_str[0] == '-') - 1) / 3;

            size_t res_len = str_len + fill_num + comma_num;
            char char_vec[res_len];

            // skip nums at the end if any
            size_t end = std::min(whole_len + decimal_precision, str_len - 1);
            size_t index = res_len - 1;
            // fill 0s at the end
            if (fill_num > 0)
            {
                memset(&char_vec[res_len - fill_num], '0', fill_num);
                index -= fill_num;
            }

            // add . for int if need to fill
            if (fill_num > 0 && end < whole_len)
            {
                char_vec[index + 1] = '.';
            }

            // fill decimal if exist
            for (size_t i = end; i >= whole_len; i--)
            {
                char_vec[index] = src_str[i];
                index--;
            }

            // TODO: add optimizations
            // a) write 4 bytes each time:
            // uint32_t *dest;
            // *dest++ = ((*(uint32_t*)src) & 0x00FFFFFF) | ',';
            // src += 3;
            // b) AVX pdep https://www.felixcloutier.com/x86/pdep
            // c) AVX multiple round 128/256 bit shift / with mask
            // e.g. 01234567...
            // 012,XXXXXXXXXXXX
            //  XXX345,XXXXXXXXX (2nd shift)
            //   XXXXXX567,XXXXXX (3rd shift)
            // ...
            // ---------------------------- combine above result
            // 012,345,567,

            // fill whole numbers and follow en_US locale
            for (size_t i = 1; i < whole_len; i++)
            {
                char_vec[index] = src_str[whole_len - i];
                index--;
                if (i % 3 == 0)
                {
                    char_vec[index] = ',';
                    index--;
                }
            }

            char_vec[0] = src_str[0];
            res_col->insertData(char_vec, res_len);
        }

        return res_col;
    }

    static int getScaleArg(const ColumnsWithTypeAndName & arguments)
    {
        const IColumn & scale_column = *arguments[1].column;
        if (!isColumnConst(scale_column))
            throw Exception("Scale argument for format functions must be constant", ErrorCodes::ILLEGAL_COLUMN);

        Field scale_field = assert_cast<const ColumnConst &>(scale_column).getField();
        int scale64 = scale_field.get<Int64>();
        // only support up to 30 digits like MySQL
        scale64 = scale64 == 0 ? -1 : scale64;
        scale64 = std::min(std::max(scale64, -1), 30);
        return scale64;
    }
};

}
