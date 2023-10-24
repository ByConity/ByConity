#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractDays = FunctionDateOrDateTimeAddInterval<SubtractDaysImpl>;

REGISTER_FUNCTION(SubtractDays)
{
    factory.registerFunction<FunctionSubtractDays>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("date_sub", SubtractDaysImpl::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("subdate", SubtractDaysImpl::name, FunctionFactory::CaseInsensitive);
}

}


