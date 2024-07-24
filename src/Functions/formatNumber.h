#pragma once
#include <cstddef>
#include <locale>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/FunctionsRound.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include <boost/container/static_vector.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

struct FormatNumberImpl
{
    static ColumnPtr
    formatExecute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_row_counts, ContextPtr context)
    {
        const DataTypePtr & data_type = arguments[0].type;
        // Assuming 2^256 is largest whole number, log_10(2^256) = 78 
        static size_t constexpr whole_number_max_len = 78;
        
        std::locale lc;
        std::string_view locale_name = "en_US.utf8";
        if (arguments.size() == 3)
        {
            const ColumnConst * locale_str = checkAndGetColumnConst<ColumnString>(arguments[2].column.get());
            if (locale_str == nullptr)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected const string");
            locale_name = locale_str->getDataColumn().getDataAt(0).toView();
        }

        try 
        {
            lc = std::locale(locale_name.data());
        } catch (const std::runtime_error &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Locale {} not supported", locale_name);
        }
        
        const std::numpunct<char> & lc_numeric = std::use_facet<std::numpunct<char>>(lc);
        const char decimal_point = lc_numeric.decimal_point();
        const char thousands_sep = lc_numeric.thousands_sep();
        const std::string grouping = [&lc_numeric]{
            std::string g = lc_numeric.grouping();
            if (g.empty()) g.push_back(0);
            return g;
        }();
        boost::container::static_vector<size_t, whole_number_max_len + 1> prefix_sum_(grouping.size() + 1);
        size_t *prefix_sum = &prefix_sum_[1];
        for (size_t i = 0; i < grouping.size(); ++i)
        {
            if (grouping[i] == 0 || grouping[i] == CHAR_MAX)
            {
                prefix_sum[i] = std::numeric_limits<size_t>::max();
                break;
            }
            prefix_sum[i] = prefix_sum[i - 1] + grouping[i];
        }

        while (prefix_sum_.back() < whole_number_max_len)
            prefix_sum_.push_back(prefix_sum_.back() + grouping.back());

        std::string whole_part_template;
        struct NumericLUTEntry
        {
            unsigned char whole_part_size;
            unsigned char comma_num;
        };
        std::array<NumericLUTEntry, whole_number_max_len + 1> table;
        {
            size_t i = 0;
            for (size_t whole_number_length = 1; whole_number_length < table.size(); ++whole_number_length)
            {
                NumericLUTEntry & entry = table[whole_number_length];
            
                if (prefix_sum[i] < whole_number_length)
                {
                    ++i;
                    whole_part_template.push_back(thousands_sep);
                }
                whole_part_template.push_back('\0');
                entry.whole_part_size = static_cast<unsigned char>(i + whole_number_length);
                entry.comma_num = static_cast<unsigned char>(i);
            }
        }
        std::reverse(whole_part_template.begin(), whole_part_template.end());

        // round raw numbers
        auto func_builder_round = FunctionFactory::instance().get("round", context);
        ColumnsWithTypeAndName args_stripped_of_locale{arguments[0], arguments[1]};
        auto rounded_data_ptr = func_builder_round->build(args_stripped_of_locale)->execute(args_stripped_of_locale, data_type, input_row_counts);

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
            const size_t whole_len_no_sign = whole_len - (src_str[0] == '-');
            const NumericLUTEntry table_entry = table.at(whole_len_no_sign);

            const size_t res_len = str_len + fill_num + table_entry.comma_num;
            char char_vec[res_len];

            // skip nums at the end if any
            const size_t end = std::min(whole_len + decimal_precision, str_len - 1);
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
                char_vec[index + 1] = decimal_point;
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

            memcpy(char_vec + (src_str[0] == '-'), &*(whole_part_template.rbegin() + table_entry.whole_part_size - 1), table_entry.whole_part_size);
            for (size_t i = 1; i < whole_len; i++)
            {
                char_vec[index] = src_str[whole_len - i];
                index--;
                if (char_vec[index] == thousands_sep)
                    index--;
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
        int scale64 = scale_column.getInt(0);
        // only support up to 30 digits like MySQL
        scale64 = scale64 == 0 ? -1 : scale64;
        scale64 = std::min(std::max(scale64, -1), 30);
        return scale64;
    }
};

}
