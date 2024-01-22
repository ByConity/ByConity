#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddNanoseconds = FunctionDateOrDateTimeAddInterval<AddNanosecondsImpl>;
REGISTER_FUNCTION(AddNanoseconds)
{
    factory.registerFunction<FunctionAddNanoseconds>(FunctionFactory::CaseInsensitive);
}

using FunctionAddMicroseconds = FunctionDateOrDateTimeAddInterval<AddMicrosecondsImpl>;
REGISTER_FUNCTION(AddMicroseconds)
{
    factory.registerFunction<FunctionAddMicroseconds>(FunctionFactory::CaseInsensitive);
}

using FunctionAddMilliseconds = FunctionDateOrDateTimeAddInterval<AddMillisecondsImpl>;
REGISTER_FUNCTION(AddMilliseconds)
{
    factory.registerFunction<FunctionAddMilliseconds>(FunctionFactory::CaseInsensitive);
}

}


