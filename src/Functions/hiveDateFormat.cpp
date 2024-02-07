#include <Common/Exception.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/formatDateTime.cpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNSUPPORTED_PARAMETER;
    extern const int BAD_ARGUMENTS;
}

/*
 * This function is like date_format in hive.
 */
class FunctionHiveDateFormat : public FunctionFormatDateTime
{
    static constexpr std::string_view PATTERN_CHARS = "GyMdkHmsSEDFwWahKzZYuXL";
    static constexpr int TAG_ASCII_CHAR = 100;

    enum PATTERN {
        ERA = 0,                    // G
        YEAR,                       // y
        MONTH,                      // M
        DAY_OF_MONTH,               // d
        HOUR_OF_DAY1,               // k
        HOUR_OF_DAY0,               // H
        MINUTE,                     // m
        SECOND,                     // s
        MILLISECOND,                // S
        DAY_OF_WEEK,                // E
        DAY_OF_YEAR,                // D
        DAY_OF_WEEK_IN_MONTH,       // F
        WEEK_OF_YEAR,               // w
        WEEK_OF_MONTH,              // W
        AM_PM,                      // a
        HOUR1,                      // h
        HOUR0,                      // K
        ZONE_NAME,                  // z
        ZONE_VALUE,                 // Z
        WEEK_YEAR,                  // Y
        ISO_DAY_OF_WEEK,            // u
        ISO_ZONE,                   // X
        MONTH_STANDALONE            // L
    };

    void compilePattern(String & pattern, String & compiled_code) const
    {
        auto encode = [](int tag, int length, String & buffer)
        {
            if (length < 255)
            {
                buffer += static_cast<char>(tag);
                buffer += static_cast<char>(length);
            }
            else
                throw Exception("Illegal date format pattern. ", ErrorCodes::BAD_ARGUMENTS);
        };

        size_t length = pattern.size();
        int count = 0;
        int last_tag = -1;

        for (size_t i = 0; i < length; i++)
        {
            char c = pattern[i];

            if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')))
            {
                if (count != 0)
                {
                    encode(last_tag, count, compiled_code);
                    last_tag = -1;
                    count = 0;
                }

                size_t j;
                for (j = i + 1; j < length; j++)
                {
                    char d = pattern[j];
                    if ((d >= 'a' && d <= 'z') || (d >= 'A' && d <= 'Z'))
                        break;
                }

                encode(TAG_ASCII_CHAR, j - i, compiled_code);

                for (; i < j; i++)
                {
                    compiled_code += (pattern[i]);
                }
                i--;

                continue;
            }

            auto found = PATTERN_CHARS.find(c);
            if (found == String::npos)
                throw Exception(String("Unknown pattern character '") + c + "'.", ErrorCodes::UNSUPPORTED_PARAMETER);

            int tag = found;

            if (last_tag == -1 || last_tag == tag)
            {
                last_tag = tag;
                count++;
                continue;
            }

            encode(last_tag, count, compiled_code);
            last_tag = tag;
            count = 1;
        }

        if (count != 0)
            encode(last_tag, count, compiled_code);
    }

    template <typename T>
    String parsePattern(String & pattern, std::vector<FunctionFormatDateTime::Instruction<T>> & instructions) const
    {
        String compiled;
        compilePattern(pattern, compiled);

        auto add_extra_shift = [&](size_t amount) {
            if (instructions.empty())
            {
                Instruction<T> instruction;
                instruction.setMysqlFunc(&Instruction<T>::mysqlNoop);
                instructions.emplace_back(std::move(instruction));
            }
            instructions.back().extra_shift += amount;
        };

        [[maybe_unused]] auto add_literal_instruction = [&](std::string_view literal) {
            Instruction<T> instruction;
            instruction.setMysqlFunc(&Instruction<T>::mysqlLiteral);
            instruction.setLiteral(literal);
            instructions.emplace_back(std::move(instruction));
        };

        auto add_instruction_or_shift = [&](typename FunctionFormatDateTime::Instruction<T> instruction [[maybe_unused]], size_t shift) {
            if constexpr (std::is_same_v<T, UInt32>)
                instructions.emplace_back(std::move(instruction));
            else
                add_extra_shift(shift);
        };

        String out_template;

        size_t length = compiled.size();

        int tag;
        int count;

        for (size_t i = 0; i < length;)
        {
            if ((tag = compiled[i++]) == TAG_ASCII_CHAR)
            {
                count = compiled[i++];
                out_template.append(compiled, i, count);
                add_extra_shift(count);
                i += count;
            }
            else
            {
                count = compiled[i++];
                Instruction<T> instruction;
                switch (tag)
                {
                    case PATTERN::WEEK_YEAR:
                    case PATTERN::YEAR:
                        if (count != 2)
                        {
                            instruction.setMysqlFunc(&Instruction<T>::mysqlYear4);
                            instructions.emplace_back(std::move(instruction));
                            out_template += "0000";
                        }
                        else
                        {
                            instruction.setMysqlFunc(&Instruction<T>::mysqlYear2);
                            instructions.emplace_back(std::move(instruction));
                            out_template += "00";
                        }
                        break;

                    case PATTERN::MONTH:
                        instruction.setMysqlFunc(&Instruction<T>::mysqlMonth);
                        instructions.emplace_back(std::move(instruction));
                        out_template += "00";
                        break;

                    case PATTERN::DAY_OF_MONTH:
                        instruction.setMysqlFunc(&Instruction<T>::mysqlDayOfMonth);
                        instructions.emplace_back(std::move(instruction));
                        out_template += "00";
                        break;
                        break;

                    case PATTERN::HOUR_OF_DAY0:
                        instruction.setMysqlFunc(&Instruction<T>::mysqlHour24);
                        add_instruction_or_shift(instruction, 2);
                        out_template += "00";
                        break;

                    case PATTERN::HOUR0:
                        instruction.setMysqlFunc(&Instruction<T>::mysqlHour12);
                        add_instruction_or_shift(instruction, 2);
                        out_template += "00";
                        break;

                    case PATTERN::MINUTE:
                        instruction.setMysqlFunc(&Instruction<T>::mysqlMinute);
                        add_instruction_or_shift(instruction, 2);
                        out_template += "00";
                        break;

                    case PATTERN::SECOND:
                        instruction.setMysqlFunc(&Instruction<T>::mysqlSecond);
                        add_instruction_or_shift(instruction, 2);
                        out_template += "00";
                        break;

                    case PATTERN::DAY_OF_YEAR:
                        instruction.setMysqlFunc(&Instruction<T>::mysqlDayOfYear);
                        instructions.emplace_back(std::move(instruction));
                        out_template += "000";
                        break;

                    case PATTERN::AM_PM:
                        instruction.setMysqlFunc(&Instruction<T>::mysqlAMPM);
                        add_instruction_or_shift(instruction, 2);
                        out_template += "AM";
                        break;

                    default:
                        throw Exception("Not supported pattern: " + std::to_string(tag), ErrorCodes::NOT_IMPLEMENTED);
                }
            }
        }

        add_extra_shift(1);
        return out_template;
    }

public:
    static constexpr auto name = "date_format";

    static FunctionPtr create(ContextPtr context)
    {
        // In mysql dialect, use mysql date format
        if (context->getSettingsRef().dialect_type == DialectType::MYSQL)
            return std::make_shared<FunctionFormatDateTimeImpl>(false, true, true, context);
        // Have to include this check to separate hive and ck behavior
        else if (context->getSettingsRef().date_format_clickhouse)
            return std::make_shared<FunctionFormatDateTimeImpl>(false, true, true, context);
        return std::make_shared<FunctionHiveDateFormat>(context);
    }

    explicit FunctionHiveDateFormat(ContextPtr context) : FunctionFormatDateTime(false, false, false, context) {}

    String getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
            );

        if (
            !WhichDataType(arguments[0].type).isDateOrDateTime()
            && !WhichDataType(arguments[0].type).isUInt32()
            && !WhichDataType(arguments[0].type).isString()
        )
            throw Exception(
                "Illegal type " + arguments[0].type->getName() +
                " of 1 argument of function " + getName() +
                ". Should be datetime or datetime format string",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!WhichDataType(arguments[1].type).isString())
            throw Exception(
                "Illegal type " + arguments[1].type->getName() +
                " of 2 argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 3)
        {
            if (!WhichDataType(arguments[2].type).isString())
                throw Exception(
                    "Illegal type " + arguments[2].type->getName() +
                    " of 3 argument of function " + getName() + ". Must be String.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        [[maybe_unused]] const DataTypePtr & result_type,
        [[maybe_unused]] size_t input_rows_count
        ) const override
    {
        auto date_time_type = std::make_shared<DataTypeDateTime>();
        ColumnsWithTypeAndName tmp_arguments;
        tmp_arguments.emplace_back(arguments[0]); /* from data */
        if (arguments.size() == 3) /* time zone */
            tmp_arguments.emplace_back(arguments[2]);

        FunctionPtr convert = std::make_shared<FunctionToDateTime>();
        ColumnPtr times = convert->executeImpl(tmp_arguments, date_time_type, input_rows_count);

        const ColumnConst * pattern_column =
            checkAndGetColumnConst<ColumnString>(arguments[1].column.get());

        if (!pattern_column)
            throw Exception(
                "Illegal column " + arguments[1].column->getName()
                + " of second ('format') argument of function " + getName() + ". Must be constant string.",
                ErrorCodes::ILLEGAL_COLUMN);

        auto pattern = pattern_column->getValue<String>();

        std::vector<FunctionFormatDateTime::Instruction<UInt32>> instructions;
        String pattern_to_fill = parsePattern(pattern, instructions);
        size_t result_size = pattern_to_fill.size();

        auto & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);

        auto col_res = ColumnString::create();
        auto & dst_data = col_res->getChars();
        auto & dst_offsets = col_res->getOffsets();
        dst_data.resize(times->size() * (result_size + 1));
        dst_offsets.resize(times->size());

        /// Fill result with literals.
        {
            UInt8 * begin = dst_data.data();
            UInt8 * end = begin + dst_data.size();
            UInt8 * pos = begin;

            if (pos < end)
            {
                memcpy(pos, pattern_to_fill.data(), result_size + 1); /// With zero terminator.
                pos += result_size + 1;
            }

            /// Fill by copying exponential growing ranges.
            while (pos < end)
            {
                size_t bytes_to_copy = std::min(pos - begin, end - pos);
                memcpy(pos, begin, bytes_to_copy);
                pos += bytes_to_copy;
            }
        }

        auto begin = reinterpret_cast<char *>(dst_data.data());
        auto pos = begin;

        for (size_t i = 0; i < times->size(); ++i)
        {
            for (auto & instruction : instructions)
                instruction.perform(pos, (*times)[i].get<UInt32>(), 0, 0, time_zone);

            dst_offsets[i] = pos - begin;
        }

        dst_data.resize(pos - begin);

        return col_res;
    }
};

REGISTER_FUNCTION(HiveDateFormat)
{
    factory.registerFunction<FunctionHiveDateFormat>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("date_format_hive", "date_format", FunctionFactory::CaseInsensitive);
}

}
