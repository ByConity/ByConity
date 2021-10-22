#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>

namespace DB
{

//class FunctionNextDay: public IFunction
//{
//public:
//    static constexpr auto name = "nextDay";
//    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNextDay>(); }
//
//    String getName() const override { return name; }
//
//    bool useDefaultImplementationForConstants() const override { return true; }
//    size_t getNumberOfArguments() const override { return 2; }
//
//    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
//    {
//        return std::make_shared<DataTypeDate>();
//    }
//
//    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
//    {
//        const IDataType * from_type = arguments[0].type.get();
//        WhichDataType which(from_type);
//
//        if (which.isDate())
//            DateTimeAddIntervalImpl<DataTypeDate::FieldType, AddToNextDayImp>::execute(arguments, arguments);
//        else if (which.isDateTime())
//            DateTimeAddIntervalImpl<DataTypeDateTime::FieldType, AddToNextDayImp>::execute(arguments, arguments, result);
//        else if (which.isString())
//        {
//            FunctionPtr convert = std::make_shared<FunctionToDate>();
//            convert->executeImpl(block, arguments, result, size);
//            block.getByPosition(0).column = block.getByPosition(result).column;
//            DateTimeAddIntervalImpl<DataTypeDate::FieldType, AddToNextDayImp>::execute(block, arguments, result);
//        }
//        else
//            throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function next_day",
//                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
//    }
//};

using FunctionNextDay = FunctionDateOrDateTimeAddInterval<NextDayImp>;

void registerFunctionNextDay(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNextDay>();
    factory.registerAlias("next_day", NextDayImp::name);
}

}
