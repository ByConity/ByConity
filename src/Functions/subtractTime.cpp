#include <Functions/FunctionDateOrDateTimeAddInterval.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>


namespace DB
{
using FunctionSubtractTime = FunctionDateOrDateTimeAddInterval<SubtractTimeImpl>;

void registerFunctionSubtractTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractTime>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("subtime", SubtractTimeImpl::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("timediff", SubtractTimeImpl::name, FunctionFactory::CaseInsensitive);
}

}
