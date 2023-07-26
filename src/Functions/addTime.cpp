#include <Functions/FunctionDateOrDateTimeAddInterval.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
using FunctionAddTime = FunctionDateOrDateTimeAddInterval<AddTimeImpl>;

void registerFunctionAddTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddTime>(FunctionFactory::CaseInsensitive);
}

}
