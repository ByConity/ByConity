#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionMySql.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct AddTimeImpl
{
    static constexpr auto name = "addTime";

    const Decimal64::NativeType delta_scale_multiplier = 1;
    const Decimal64::NativeType base_scale_multiplier = 1;

    Decimal64::NativeType scale_change = 1;
    // Constants
    static constexpr auto SECONDS_IN_DAY = 86400L;

    AddTimeImpl(UInt32 delta_scale_ = 0, UInt32 base_scale_ = 0)
        : delta_scale_multiplier(DecimalUtils::scaleMultiplier<Decimal64>(delta_scale_))
        , base_scale_multiplier(DecimalUtils::scaleMultiplier<Decimal64>(base_scale_))
    {
        int scale_arg = base_scale_ - delta_scale_;
        if (scale_arg > 0)
            scale_change = intExp10(scale_arg);
        else
            scale_change = intExp10(-scale_arg);
    }

    inline NO_SANITIZE_UNDEFINED Decimal64::NativeType scaleAndAdd(Decimal64 & a, Decimal64 & b) const
    {
        if (delta_scale_multiplier < base_scale_multiplier)
            b *= scale_change;
        else if (delta_scale_multiplier > base_scale_multiplier)
            a *= scale_change;

        return a + b;
    }

    // Input datetime64
    inline NO_SANITIZE_UNDEFINED Decimal64::NativeType execute(DateTime64 dt, Decimal64 delta, const DateLUTImpl &) const
    {
        return scaleAndAdd(dt, delta);
    }

    // input time
    inline NO_SANITIZE_UNDEFINED Decimal64::NativeType executeTime(Decimal64 t, Decimal64 delta, const DateLUTImpl &) const
    {
        auto scale_multiplier = std::max(delta_scale_multiplier, base_scale_multiplier);
        Int64 x = scaleAndAdd(t, delta) % (SECONDS_IN_DAY * scale_multiplier);

        if (x < 0)
        {
            x += SECONDS_IN_DAY * scale_multiplier;
        }

        return x;
    }

    // input date time
    inline NO_SANITIZE_UNDEFINED Decimal64::NativeType execute(UInt32 dt, Decimal64 delta, const DateLUTImpl &) const
    {
        return dt * scale_change + delta;
    }

    // input date32
    inline NO_SANITIZE_UNDEFINED Decimal64::NativeType execute(Int32 d, Decimal64 delta, const DateLUTImpl & time_zone) const
    {
        // use default datetime64 scale
        return time_zone.fromDayNum(ExtendedDayNum(d)) * scale_change + delta;
    }

    // input date
    inline NO_SANITIZE_UNDEFINED Decimal64::NativeType execute(UInt16 d, Decimal64 delta, const DateLUTImpl & time_zone) const
    {
        return time_zone.fromDayNum(DayNum(d)) * scale_change + delta;
    }
};

struct NameAddTime
{
    static constexpr auto name = "addtime";
    static constexpr auto sign = 1;
};

struct NameSubTime
{
    static constexpr auto name = "subtime";
    static constexpr auto sign = -1;
};


template <typename NameStruct>
class FunctionAddOrSubTime : public IFunction
{
private:
    ContextPtr context;

public:
    using time_type = DecimalUtils::DecimalComponents<Decimal64>;
    static constexpr auto name = NameStruct::name;

    explicit FunctionAddOrSubTime(ContextPtr context_) : context(context_) { }

    static FunctionPtr create(ContextPtr context)
    {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return std::make_shared<IFunctionMySql>(std::make_unique<FunctionAddOrSubTime>(context));
        return std::make_shared<FunctionAddOrSubTime>(context);
    }

    ArgType getArgumentsType() const override
    {
        return ArgType::DATE_STR;
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}",
                getName(),
                toString(arguments.size()));

        if (!isDate(arguments[0]) && !isDate32(arguments[0]) && !isDateTime(arguments[0]) && !isDateTime64(arguments[0])
            && !isStringOrFixedString(arguments[0]) && !isTime(arguments[0]))
            throw Exception{
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                    + ". Should be a date or a date with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!isStringOrFixedString(arguments[1]) && !isTime(arguments[1]))
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
                    + ". Should be a date or a date with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        // Time's default scale
        uint32_t right_scale = 3;
        if (isTime(arguments[1]))
        {
            // may have lower scale
            const auto & t1 = assert_cast<const DataTypeTime &>(*arguments[1]);
            right_scale = t1.getScale();
        }

        switch (arguments[0]->getTypeId())
        {
            case TypeIndex::Date:
            case TypeIndex::Date32:
            case TypeIndex::DateTime: {
                return std::make_shared<DataTypeDateTime64>(right_scale);
            }
            case TypeIndex::DateTime64: {
                const auto & dt = assert_cast<const DataTypeDateTime64 &>(*arguments[0]);
                return std::make_shared<DataTypeDateTime64>(std::max(dt.getScale(), right_scale));
            }
            case TypeIndex::String:
            case TypeIndex::FixedString: {
                return std::make_shared<DataTypeString>();
            }
            case TypeIndex::Time: {
                const auto & t = assert_cast<const DataTypeTime &>(*arguments[0]);
                return std::make_shared<DataTypeTime>(std::max(t.getScale(), right_scale));
            }
            default: {
                throw Exception(
                    "Invalid type of 1st argument of function " + getName() + ": " + arguments[0]->getName()
                        + ", expected: Date, DateTime, DateTime64 or String.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
    }

    template <typename BaseType, typename ResultColType>
    void executeInternal(const ColumnPtr & base_arg, auto & delta_arg, IColumn * result, const DateLUTImpl & time_zone, UInt32 base_scale) const
    {
        const auto & col_base_data = typeid_cast<const BaseType &>(*base_arg).getData();
        const auto & delta_data = typeid_cast<const ColumnTime &>(*delta_arg.column).getData();
        auto * col_res = assert_cast<ResultColType *>(result);
        auto & col_res_data = col_res->getData();

        size_t size = col_base_data.size();
        col_res_data.resize(size);

        const auto & delta_type = assert_cast<const DataTypeTime &>(*delta_arg.type);
        const AddTimeImpl addTimeImpl(delta_type.getScale(), base_scale);

        if constexpr (std::is_same_v<ResultColType, ColumnTime>)
        {
            for (size_t i = 0; i < size; ++i)
                col_res_data[i] = addTimeImpl.executeTime(col_base_data[i], NameStruct::sign * delta_data[i], time_zone);
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
                col_res_data[i] = addTimeImpl.execute(col_base_data[i], NameStruct::sign * delta_data[i], time_zone);
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        const auto &base_col = arguments[0].column;
        const auto &base_type = arguments[0].type;
        auto delta_arg = arguments[1];

        auto result_col = result_type->createColumn();

        if (isStringOrFixedString(arguments[1].type))
        {
            auto to_time = FunctionFactory::instance().get("toTimeType", context);
            auto to_time_col = to_time->build({arguments[1]})->execute({arguments[1]}, std::make_shared<DataTypeTime>(3), input_rows_count);
            ColumnWithTypeAndName time_col(to_time_col, std::make_shared<DataTypeTime>(3), "unixtime");
            delta_arg = std::move(time_col);
        }

        switch (base_type->getTypeId())
        {
            case TypeIndex::Date: {
                const auto & time_zone = DateLUT::sessionInstance();
                executeInternal<ColumnDate, ColumnDateTime64>(base_col, delta_arg, result_col.get(), time_zone, 0);
                break;
            }
            case TypeIndex::Date32: {
                const auto & time_zone = DateLUT::sessionInstance();
                executeInternal<ColumnDate32, ColumnDateTime64>(base_col, delta_arg, result_col.get(), time_zone, 0);
                break;
            }
            case TypeIndex::DateTime: {
                const auto & time_zone = assert_cast<const DataTypeDateTime *>(base_type.get())->getTimeZone();
                executeInternal<ColumnDateTime, ColumnDateTime64>(base_col, delta_arg, result_col.get(), time_zone, 0);
                break;
            }
            case TypeIndex::DateTime64: {
                const auto & dt = assert_cast<const DataTypeDateTime64 &>(*arguments[0].type);
                const auto & time_zone = dt.getTimeZone();
                executeInternal<ColumnDateTime64, ColumnDateTime64>(base_col, delta_arg, result_col.get(), time_zone, dt.getScale());
                break;
            }
            case TypeIndex::Time: {
                const auto & t = assert_cast<const DataTypeTime &>(*arguments[0].type);
                const auto & time_zone = DateLUT::sessionInstance();
                executeInternal<ColumnTime, ColumnTime>(base_col, delta_arg, result_col.get(), time_zone, t.getScale());
                break;
            }
            // disable string for now
            case TypeIndex::String:
            case TypeIndex::FixedString:
            default: {
                throw Exception(
                    "Invalid type of 1st argument of function " + getName() + ": " + base_type->getName()
                        + ", expected: Date, DateTime, DateTime64 or String.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        return result_col;
    }
};

using FunctionAddTime = FunctionAddOrSubTime<NameAddTime>;
using FunctionSubTime = FunctionAddOrSubTime<NameSubTime>;

REGISTER_FUNCTION(AddTime)
{
    factory.registerFunction<FunctionAddTime>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionSubTime>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("subtractTime", NameSubTime::name, FunctionFactory::CaseInsensitive);
}
}
