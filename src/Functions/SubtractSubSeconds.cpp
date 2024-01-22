#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractNanoseconds = FunctionDateOrDateTimeAddInterval<SubtractNanosecondsImpl>;
REGISTER_FUNCTION(SubtractNanoseconds)
{
    factory.registerFunction<FunctionSubtractNanoseconds>(FunctionFactory::CaseInsensitive);
}

using FunctionSubtractMicroseconds = FunctionDateOrDateTimeAddInterval<SubtractMicrosecondsImpl>;
REGISTER_FUNCTION(SubtractMicroseconds)
{
    factory.registerFunction<FunctionSubtractMicroseconds>(FunctionFactory::CaseInsensitive);
}

using FunctionSubtractMilliseconds = FunctionDateOrDateTimeAddInterval<SubtractMillisecondsImpl>;
REGISTER_FUNCTION(SubtractMilliseconds)
{
    factory.registerFunction<FunctionSubtractMilliseconds>(FunctionFactory::CaseInsensitive);
}

}


