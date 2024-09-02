#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>

#include <charconv>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static inline Int64 getSign(const std::string_view & delta)
{
    auto first = delta.find_first_not_of(' ');
    if (first == std::string_view::npos)
        return 0; // delta contains only whitespaces or nothing, just return 0
    return (delta[first] == '-') ? -1 : 1;
}

static inline void moveToStartOfInterval(const std::string_view & delta, std::reverse_iterator<const char *> & iter)
{
    iter = std::prev(std::find_if_not(iter, delta.crend(), static_cast<int(*)(int)>(std::isdigit)), 1);
}

static inline void moveToEndOfInterval(const std::string_view & delta, std::reverse_iterator<const char *> & iter)
{
    iter = std::find_if(iter, delta.crend(), static_cast<int(*)(int)>(std::isdigit));
}

static inline void getIntervalValue(const std::string_view & delta, std::reverse_iterator<const char *> & iter, Int64 & interval)
{
    moveToEndOfInterval(delta, iter);
    if (iter != delta.crend())
    {
        auto end = iter;

        moveToStartOfInterval(delta, iter);

        auto pStart = &(*iter);
        auto size = iter - end + 1;

        #ifndef DEBUG
            std::from_chars(pStart, pStart + size, interval);
        #else
            chassert(iter != delta.crend());
            auto [ptr, ec] = std::from_chars(pStart, pStart + size, interval);
            if (ec == std::errc())
                std::cout << "Result: " << interval << ", ptr -> " << std::quoted(ptr) << '\n';
            else if (ec == std::errc::invalid_argument)
                std::cout << "That isn't a number.\n";
            else if (ec == std::errc::result_out_of_range)
                std::cout << "This number is larger than an Int64.\n";
        #endif

        iter = std::next(iter); // Move to the next set of characters to read
    }
    else
    {
        interval = 0;
    }
}

static inline void checkLast(const std::string_view & delta, std::reverse_iterator<const char *> & iter)
{
    moveToEndOfInterval(delta, iter);
    if (iter != delta.crend())
        throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Too many interval values provided {}", delta);
}

struct ConvertMinuteSecondToSecondImpl
{
    static constexpr auto name = "convertMinuteSecondToSecond";

    static inline Int64 execute(const std::string_view & delta, std::reverse_iterator<const char *> & iter)
    {
        Int64 second, minute;

        getIntervalValue(delta, iter, second);
        getIntervalValue(delta, iter, minute);

        return minute * 60 + second; // convert everything to seconds
    }
};

struct ConvertHourSecondToSecondImpl
{
    static constexpr auto name = "convertHourSecondToSecond";

    static inline Int64 execute(const std::string_view & delta, std::reverse_iterator<const char *> & iter)
    {
        Int64 second, minute, hour;

        getIntervalValue(delta, iter, second);
        getIntervalValue(delta, iter, minute);
        getIntervalValue(delta, iter, hour);

        return hour * 3600 + minute * 60 + second; // convert everything to seconds
    }
};

struct ConvertHourMinuteToMinuteImpl
{
    static constexpr auto name = "convertHourMinuteToMinute";

    static inline Int64 execute(const std::string_view & delta, std::reverse_iterator<const char *> & iter)
    {
        Int64 minute, hour;

        getIntervalValue(delta, iter, minute);
        getIntervalValue(delta, iter, hour);

        return hour * 60 + minute; // convert everything to minutes
    }
};

struct ConvertDaySecondToSecondImpl
{
    static constexpr auto name = "convertDaySecondToSecond";

    static inline Int64 execute(const std::string_view & delta, std::reverse_iterator<const char *> & iter)
    {
        Int64 second, minute, hour, day;

        getIntervalValue(delta, iter, second);
        getIntervalValue(delta, iter, minute);
        getIntervalValue(delta, iter, hour);
        getIntervalValue(delta, iter, day);

        return day * 86400 + hour * 3600 + minute * 60 + second; // convert everything to seconds
    }
};

struct ConvertDayMinuteToMinuteImpl
{
    static constexpr auto name = "convertDayMinuteToMinute";

    static inline Int64 execute(const std::string_view & delta, std::reverse_iterator<const char *> & iter)
    {
        Int64 minute, hour, day;

        getIntervalValue(delta, iter, minute);
        getIntervalValue(delta, iter, hour);
        getIntervalValue(delta, iter, day);

        return day * 1440 + hour * 60 + minute; // convert everything to minutes
    }
};

struct ConvertDayHourToHourImpl
{
    static constexpr auto name = "convertDayHourToHour";

    static inline Int64 execute(const std::string_view & delta, std::reverse_iterator<const char *> & iter)
    {
        Int64 hour, day;

        getIntervalValue(delta, iter, hour);
        getIntervalValue(delta, iter, day);

        return day * 24 + hour; // convert everything to hours
    }
};

struct ConvertYearMonthToMonthImpl
{
    static constexpr auto name = "convertYearMonthToMonth";

    static inline Int64 execute(const std::string_view & delta, std::reverse_iterator<const char *> & iter)
    {
        Int64 month, year;

        getIntervalValue(delta, iter, month);
        getIntervalValue(delta, iter, year);

        return year * 12 + month; // convert everything to months
    }
};

template <typename ConvertImpl>
class ConvertCombinedInterval : public IFunction
{
public:
    static constexpr auto name = ConvertImpl::name;
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<ConvertCombinedInterval>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t size = arguments.size();
        if (size != 1)
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(size) + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypePtr arg = arguments[0];
        if (!isStringOrFixedString(arg))
        {
            throw Exception{
                "Illegal type " + arg->getName() + " of argument " + std::to_string(0) + " of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        MutableColumnPtr res{result_type->createColumn()};
        auto & internal_data = typeid_cast<ColumnVector<Int64> *>(res.get())->getData();
        internal_data.resize(input_rows_count);

        for (size_t r = 0; r < input_rows_count; ++r)
        {
            std::string_view delta = arguments[0].column->getDataAt(r).toView();
            std::reverse_iterator<const char *> iter = delta.crbegin();

            Int64 sign = getSign(delta);
            internal_data[r] = sign * ConvertImpl::execute(delta, iter);

            checkLast(delta, iter);
        }
        return res;
    }
};

using FunctionConvertHourSecondToSecond = ConvertCombinedInterval<ConvertHourSecondToSecondImpl>;
using FunctionConvertHourMinuteToMinute = ConvertCombinedInterval<ConvertHourMinuteToMinuteImpl>;
using FunctionConvertMinuteSecondToSecond = ConvertCombinedInterval<ConvertMinuteSecondToSecondImpl>;
using FunctionConvertDaySecondToSecond = ConvertCombinedInterval<ConvertDaySecondToSecondImpl>;
using FunctionConvertDayMinuteToMinute = ConvertCombinedInterval<ConvertDayMinuteToMinuteImpl>;
using FunctionConvertDayHourToHour = ConvertCombinedInterval<ConvertDayHourToHourImpl>;
using FunctionConvertYearMonthToMonth = ConvertCombinedInterval<ConvertYearMonthToMonthImpl>;

REGISTER_FUNCTION(ConvertCombinedIntervals)
{
    factory.registerFunction<FunctionConvertHourSecondToSecond>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionConvertHourMinuteToMinute>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionConvertMinuteSecondToSecond>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionConvertDaySecondToSecond>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionConvertDayMinuteToMinute>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionConvertDayHourToHour>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionConvertYearMonthToMonth>(FunctionFactory::CaseInsensitive);
}

}
