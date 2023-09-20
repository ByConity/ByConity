#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

class FunctionCurrentCatalog : public IFunction
{
    const String catalog_name;

public:
    static constexpr auto name = "currentCatalog";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCurrentCatalog>(context->getCurrentCatalog());
    }

    explicit FunctionCurrentCatalog(const String & catalog_name_) : catalog_name{catalog_name_}
    {
    }

    String getName() const override
    {
        return name;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, catalog_name.empty() ? "cnch" : catalog_name);
    }
};

}

void registerFunctionCurrentCatalog(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCurrentCatalog>();
    factory.registerFunction<FunctionCurrentCatalog>("CATALOG", FunctionFactory::CaseInsensitive);
}

}
