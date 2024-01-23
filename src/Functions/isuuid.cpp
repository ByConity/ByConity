#include <cstddef>
#include <Functions/FunctionFactory.h>
#include <common/types.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Functions/MatchImpl.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionIsUuid : public IFunction
{
public:
    static constexpr auto name = "is_uuid";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsUuid>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr & column = arguments[0].column;
        auto col_res = ColumnUInt8::create();
        ColumnUInt8::Container & vec_res = col_res->getData();
        UUID tmp;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            const ColumnString::Chars & data = col->getChars();
            const ColumnString::Offsets & offsets = col->getOffsets();
            vec_res.reserve(col->size());
            size_t prev_offset = 0;

            for (size_t offset : offsets)
            {

                ReadBufferFromMemory buff(reinterpret_cast<const char *>(&data[prev_offset]), offset - prev_offset - 1);
                vec_res.push_back(tryReadUUIDText(tmp, buff));
                prev_offset = offset;
            }
            return col_res;
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            const ColumnString::Chars & data = col_fixed->getChars();
            size_t n = col_fixed->getN();
            vec_res.reserve(col_fixed->size());

            for (size_t i = 0; i < data.size(); i += n)
            {
                ReadBufferFromMemory buff(reinterpret_cast<const char *>(&data[i]), n);
                vec_res.push_back(tryReadUUIDText(tmp, buff));
            }

            return col_res;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                            + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_COLUMN);
    }
};

REGISTER_FUNCTION(FunctionIsUuid)
{
    factory.registerFunction<FunctionIsUuid>(FunctionFactory::CaseInsensitive);
}

}
