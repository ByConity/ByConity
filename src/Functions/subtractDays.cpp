#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractDays = FunctionDateOrDateTimeAddInterval<SubtractDaysImpl>;

void registerFunctionSubtractDays(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractDays>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("date_sub", SubtractDaysImpl::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("subdate", SubtractDaysImpl::name, FunctionFactory::CaseInsensitive);
}

}


