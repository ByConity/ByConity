#include <Functions/FunctionsJSON.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/**
 * For function get_json_object, the arguments format like '$.name_1[0][1].name_2'
 * Here, we parse the arguments and convert them into the form of the ClickHouse built-in json functions
 * For Example:
 *  '$.[0].name_1' => {0, 'name_1'}
 *  '$.name_1[0][1].name_2' => {'name_1', 0, 1, 'name_2'}
 *
 * but there are some limitations
 *  1. The pattern should be starts with '$', otherwise the result will be empty
 *  2. The asterisk('*'), like '$.name_1[*]' is not supported now.
 */

ColumnsWithTypeAndName FunctionJSONHelpers::parserArguments(const ColumnsWithTypeAndName & columns, size_t column_index)
{
    auto column = columns.at(column_index);

    if (!column.column || !isString(column.type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The {}-th argument for function get_json_object should be ConstString", column_index);

    String data;
    if (isColumnConst(*column.column))
        data = assert_cast<const ColumnConst &>(*column.column).getValue<String>();
    else if (!column.column->empty())
        data = assert_cast<const ColumnString &>(*column.column).getDataAt(0).toString();
    else
        return columns; /// Empty string, for example: select get_json_object(json_string, pattern) from empty_table

    std::vector<std::string> values;
    boost::split(values, data, boost::is_any_of(" ."), boost::token_compress_on);

    ColumnsWithTypeAndName res;
    res.reserve(columns.size());
    res.insert(res.end(), columns.begin(), columns.begin() + column_index);

    auto emplace_string = [&](const auto & value)
    {
        auto result_type = std::make_shared<DataTypeString>();
        auto result_column = result_type->createColumnConst(1, value);
        res.emplace_back(result_column, result_type, "fake_column");
    };

    auto emplace_index = [&](const auto & value)
    {
        auto result_type = std::make_shared<DataTypeInt32>();
        /// index in get_object_json is start from 0,
        auto result_column = result_type->createColumnConst(1, value + 1);
        res.emplace_back(result_column, result_type, "fake_column");
    };

    if (values.empty() || values.front() != "$")
    {
        /// FIXME: How to handle pattern strings that do not begin with `$`
        emplace_string("");
    }
    else
    {
        /// The first character should be '$', then we should skip it.
        for (size_t i = 1; i < values.size(); ++i)
        {
            const auto & value = values[i];
            /// parse 'column_name[0][1]'
            if (auto pos = value.find('['); pos != std::string_view::npos)
            {
                if (pos != 0)
                    emplace_string(value.substr(0, pos));

                Int32 index_value = 0;
                Int32 l_bracket = 0, r_bracket = 0;
                for (size_t index = pos; index < value.size(); ++index)
                {
                    if (isNumericASCII(value[index]))
                    {
                        index_value = index_value * 10 + value[index] - '0';
                    }
                    else if (value[index] == ']')
                    {
                        emplace_index(index_value);
                        ++r_bracket;
                        if (l_bracket != r_bracket)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Left bracket and Right bracket are mismatched");
                    }
                    else if (value[index] == '[')
                    {
                        index_value = 0;
                        ++l_bracket;
                    }
                    else
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected character in json function parameter, should be number, but got {}", value[index]);
                }
            }
            else
            {
                emplace_string(value);
            }
        }
    }

    if (column_index + 1 < columns.size())
        res.insert(res.end(), columns.begin() + column_index + 1, columns.end());

    return res;
}

std::vector<FunctionJSONHelpers::Move> FunctionJSONHelpers::prepareMoves(const char * function_name, const ColumnsWithTypeAndName & columns, size_t first_index_argument, size_t num_index_arguments)
{
    std::vector<Move> moves;
    moves.reserve(num_index_arguments);
    for (const auto i : collections::range(first_index_argument, first_index_argument + num_index_arguments))
    {
        const auto & column = columns[i];
        if (!isString(column.type) && !isInteger(column.type))
            throw Exception{"The argument " + std::to_string(i + 1) + " of function " + String(function_name)
                                + " should be a string specifying key or an integer specifying index, illegal type: " + column.type->getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (column.column && isColumnConst(*column.column))
        {
            const auto & column_const = assert_cast<const ColumnConst &>(*column.column);
            if (isString(column.type))
                moves.emplace_back(MoveType::ConstKey, column_const.getValue<String>());
            else
                moves.emplace_back(MoveType::ConstIndex, column_const.getInt(0));
        }
        else
        {
            if (isString(column.type))
                moves.emplace_back(MoveType::Key, "");
            else
                moves.emplace_back(MoveType::Index, 0);
        }
    }
    return moves;
}

size_t FunctionJSONHelpers::calculateMaxSize(const ColumnString::Offsets & offsets)
{
    size_t max_size = 0;
    for (const auto i : collections::range(0, offsets.size()))
    {
        size_t size = offsets[i] - offsets[i - 1];
        if (max_size < size)
            max_size = size;
    }
    if (max_size)
        --max_size;
    return max_size;
}


REGISTER_FUNCTION(JSON)
{
    factory.registerFunction<JSONOverloadResolver<NameJSONHas, JSONHasImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameIsValidJSON, IsValidJSONImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONLength, JSONLengthImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONKey, JSONKeyImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONType, JSONTypeImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractInt, JSONExtractInt64Impl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractUInt, JSONExtractUInt64Impl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractFloat, JSONExtractFloat64Impl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractBool, JSONExtractBoolImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractString, JSONExtractStringImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtract, JSONExtractImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractKeysAndValues, JSONExtractKeysAndValuesImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractRaw, JSONExtractRawImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractArrayRaw, JSONExtractArrayRawImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractKeysAndValuesRaw, JSONExtractKeysAndValuesRawImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractKeys, JSONExtractKeysImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONUnquote, JSONUnquoteImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameGetJSONObject, GetJsonObjectImpl>>(FunctionFactory::CaseSensitiveness::CaseInsensitive);
}

}
