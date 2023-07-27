#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddDays = FunctionDateOrDateTimeAddInterval<AddDaysImpl>;

void registerFunctionAddDays(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddDays>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("date_add", AddDaysImpl::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("adddate", AddDaysImpl::name, FunctionFactory::CaseInsensitive);
}

}


