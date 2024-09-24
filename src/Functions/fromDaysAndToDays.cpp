#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/IFunctionMySql.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/TransformDateTime64.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    /// GREGORIAN representation of 1970-1-1
    constexpr Int32 GREGORIAN_OFFSET = 719528;
    /// GREGORIAN representation of 1900-1-1
    constexpr Int32 GREGORIAN_MIN_VALUE = GREGORIAN_OFFSET - DAYNUM_OFFSET_EPOCH;

    class FunctionFromDaysImpl : public IFunction
    {
    public:
        static constexpr auto name = "from_days";

        explicit FunctionFromDaysImpl(ContextPtr context_) : context(context_)
        {
        }

        static FunctionPtr create(ContextPtr context)
        {
            if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
                return std::make_shared<IFunctionMySql>(std::make_unique<FunctionFromDaysImpl>(context));
            return std::make_shared<FunctionFromDaysImpl>(context);
        }

        ArgType getArgumentsType() const override { return ArgType::NUMBERS; }

        String getName() const override
        {
            return name;
        }
        bool useDefaultImplementationForConstants() const override
        {
            return true;
        }
        size_t getNumberOfArguments() const override
        {
            return 1;
        }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (arguments.size() != 1)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}",
                    getName(),
                    toString(arguments.size()));

            if (!isNumber(arguments[0]) && !isStringOrFixedString(arguments[0]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The argument of function {} must be number or a string literal of an number",
                    getName());

            return std::make_shared<DataTypeDate32>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            ColumnPtr col = arguments[0].column;
            if (WhichDataType(col->getDataType()).isStringOrFixedString())
            {
                auto to_int = FunctionFactory::instance().get("toInt64", context);
                col = to_int->build(arguments)->execute(arguments, std::make_shared<DataTypeInt64>(), input_rows_count);
            }
            const TypeIndex & col_type = col->getDataType();

            ColumnPtr res;

            auto call = [&](const auto & types) -> bool {
                using Types = std::decay_t<decltype(types)>;
                using Type = typename Types::RightType;
                using ColVecType = ColumnVector<Type>;

                res = execute<ColVecType, Type>(col.get());
                return true;
            };
            /// All integral types are supported
            if (!callOnBasicType<void, true, true, false, false>(col_type, call))
                throw Exception("Wrong call for " + getName() + " with " + getTypeName(col_type), ErrorCodes::ILLEGAL_COLUMN);

            return res;
        }

    private:
        ContextPtr context;

        template <typename ColVecType, typename T>
        static ColumnPtr execute(const IColumn * column)
        {
            const auto * col_from = checkAndGetColumn<ColVecType>(column);

            static const Int32 daynum_min_offset = -static_cast<Int32>(DateLUT::sessionInstance().getDayNumOffsetEpoch());

            MutableColumnPtr res = DataTypeDate32().createColumn();
            auto & res_data = dynamic_cast<ColumnVector<Int32> *>(res.get())->getData();

            /// Max value of UInt16 is 65535 (0179-06-06), which is far smaller than the lower bound of
            /// ClickHouse Date family (1900-01-01). Therefore, we can safely return with a const.
            if constexpr (
                std::is_same_v<
                    T,
                    std::int8_t> || std::is_same_v<T, std::int16_t> || std::is_same_v<T, std::uint8_t> || std::is_same_v<T, std::uint16_t>)
            {
                res_data.emplace_back(daynum_min_offset);
                return ColumnConst::create(std::move(res), col_from->size());
            }
            else
            {
                const auto & data = col_from->getData();
                res_data.resize(data.size());
                for (size_t i = 0; i < col_from->size(); i++)
                {
                    /// Following the ClickHouse's manner of Date processing, we replace the out bound values
                    /// with the lower/upper bound. This is different from the behavior of MySQL.
                    if (data[i] <= GREGORIAN_MIN_VALUE)
                        res_data[i] = daynum_min_offset;
                    else
                    {
                        Int32 day_num = data[i] - GREGORIAN_OFFSET;
                        res_data[i] = std::min(day_num, DATE_LUT_MAX_EXTEND_DAY_NUM - 1);
                    }
                }
                return res;
            }
        }
    };

    class FunctionToDaysImpl : public IFunction
    {
    public:
        static constexpr auto name = "to_days";

        explicit FunctionToDaysImpl(ContextPtr context_) : context(context_) { }

        static FunctionPtr create(ContextPtr context)
        {
            if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
                return std::make_shared<IFunctionMySql>(std::make_unique<FunctionToDaysImpl>(context));
            return std::make_shared<FunctionToDaysImpl>(context);
        }

        ArgType getArgumentsType() const override { return ArgType::DATES; }

        String getName() const override { return name; }
        bool useDefaultImplementationForConstants() const override { return true; }
        size_t getNumberOfArguments() const override { return 1; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (arguments.size() != 1)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}",
                    getName(),
                    toString(arguments.size()));

            WhichDataType which_first(arguments[0]->getTypeId());

            if (!which_first.isDateOrDate32() && !which_first.isDateTime() && !which_first.isDateTime64() && !which_first.isTime()
                && !which_first.isStringOrFixedString())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument of function {} must be Date, or DateTime or String", getName());

            return std::make_shared<DataTypeInt32>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            ColumnPtr col = arguments[0].column;
            DataTypePtr from_type = arguments[0].type;
            WhichDataType which(from_type);

            if (which.isStringOrFixedString() || which.isTime())
            {
                auto to_datetime = FunctionFactory::instance().get("toDateTime64", context);
                const auto scale_type = std::make_shared<DataTypeUInt8>();
                const auto scale_col = scale_type->createColumnConst(input_rows_count, Field(0));
                ColumnWithTypeAndName scale_arg{std::move(scale_col), std::move(scale_type), "scale"};
                ColumnsWithTypeAndName args{arguments[0], std::move(scale_arg)};
                col = to_datetime->build(args)->execute(args, std::make_shared<DataTypeDateTime64>(0), input_rows_count);
                from_type = std::make_shared<DataTypeDateTime64>(0);
                which.idx = TypeIndex::DateTime64;
            }


            MutableColumnPtr res_ptr = ColumnVector<Int32>::create();
            auto & res_data = dynamic_cast<ColumnVector<Int32> *>(res_ptr.get())->getData();

            if (which.isDateTime64())
            {
                /// DateTime64 is backed by Decimal rather than pure integer, we need to take care of the scale.
                auto type = dynamic_cast<const DataTypeDateTime64 *>(from_type.get());
                chassert(type != nullptr);
                auto scale = type->getScale();
                // coverity[var_deref_model]
                const auto transform = TransformDateTime64<ToDate32Impl>(scale);
                executeType<ColumnDecimal<DateTime64>>(res_data, *col, transform);
            }
            else if (which.isDate())
                executeType<ColumnVector<UInt16>>(res_data, *col, ToDate32Impl{});
            else if (which.isDate32())
                executeType<ColumnVector<Int32>>(res_data, *col, ToDate32Impl{});
            else if (which.isDateTime())
                executeType<ColumnVector<UInt32>>(res_data, *col, ToDate32Impl{});
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument of function {} must be Date, DateTime or String", getName());

            return res_ptr;
        }

    private:
        ContextPtr context;

        template <typename ColType, typename Transform>
        void executeType(PaddedPODArray<Int32> & res_data, const IColumn & src, Transform transform) const
        {
            const auto * col = checkAndGetColumn<ColType>(src);
            if (col == nullptr)
                throw Exception("Column type does not match to the data type", ErrorCodes::ILLEGAL_COLUMN);

            const auto & timezone = DateLUT::sessionInstance();
            auto & data = col->getData();
            const auto row_size = data.size();
            res_data.resize(row_size);
            for (size_t i = 0; i < row_size; i++)
            {
                res_data[i] = transform.execute(data[i], timezone) + GREGORIAN_OFFSET;
            }
        }
    };
}

REGISTER_FUNCTION(FromDays)
{
    factory.registerFunction<FunctionFromDaysImpl>(FunctionFactory::CaseInsensitive);
}

REGISTER_FUNCTION(ToDays)
{
    factory.registerFunction<FunctionToDaysImpl>(FunctionFactory::CaseInsensitive);
}

}
