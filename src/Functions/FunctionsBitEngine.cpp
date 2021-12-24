#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBitEngine.h>

namespace DB
{

void registerFunctionsBitEngine(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitEngineDecode>();
    factory.registerFunction<FunctionDecodeNonBitEngineColumn>();
    factory.registerFunction<FunctionEncodeNonBitEngineColumn>();
    factory.registerFunction<FunctionArrayToBitmapWithEncode>();
    factory.registerFunction<FunctionBitmapToArrayWithDecode>();
    factory.registerFunction<FunctionEncodeBitmap>();
    factory.registerFunction<FunctionDecodeBitmap>();
}
}
