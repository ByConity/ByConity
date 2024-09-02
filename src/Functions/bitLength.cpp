#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Calculates the length of a string in bits.
  */
struct BitLengthImpl
{
    static constexpr auto is_fixed_to_constant = true;

    static void vector(const ColumnString::Chars & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = (offsets[i] - 1 - offsets[i - 1]) * 8;
    }

    static void vectorFixedToConstant(const ColumnString::Chars & /*data*/, size_t n, UInt64 & res)
    {
        res = n * 8;
    }

    static void vectorFixedToVector(const ColumnString::Chars & /*data*/, size_t /*n*/, PaddedPODArray<UInt64> & /*res*/)
    {
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = (offsets[i] - offsets[i - 1]) * 8;
    }

    static void map(const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res, bool /*is_mysql*/)
    {
        array(offsets, res);
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function bit_length to UUID argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    [[noreturn]] static void ipv6(const ColumnIPv6::Container &, size_t &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function bit_length to IPv6 argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    [[noreturn]] static void ipv4(const ColumnIPv4::Container &, size_t &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function bit_length to IPv4 argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    static bool isCompilable(const DataTypes & )
    {
        return false;
    }
    static llvm::Value * compileString(llvm::IRBuilderBase & , const DataTypes & , Values &  )
    {
        return nullptr;
    }
    static llvm::Value * compileFixedString(llvm::IRBuilderBase & , const DataTypes & , Values &  )
    {
        return nullptr;
    }
    static llvm::Value * compileArray(llvm::IRBuilderBase & , const DataTypes & , Values &  )
    {
        return nullptr;
    }
    static llvm::Value * compileMap(llvm::IRBuilderBase & , const DataTypes & , Values &  )
    {
        return nullptr;
    }
    static llvm::Value * compileUuid(llvm::IRBuilderBase & , const DataTypes & , Values &  )
    {
        return nullptr;
    }
};


struct NameBitLength
{
    static constexpr auto name = "bit_length";
};

using FunctionBitLength = FunctionStringOrArrayToT<BitLengthImpl, NameBitLength, UInt64>;

REGISTER_FUNCTION(BitLength)
{
    factory.registerFunction<FunctionBitLength>(FunctionFactory::CaseInsensitive);
}

}
