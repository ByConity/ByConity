#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif

namespace DB
{

/** version() - returns the current version as a string.
  */
class FunctionVersion : public IFunction
{
public:
    static constexpr auto name = "version";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionVersion>(context->getSettingsRef().dialect_type == DialectType::MYSQL);
    }

    explicit FunctionVersion(bool is_mysql_) : is_mysql(is_mysql_)
    {
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, is_mysql ? "5.1.0" : VERSION_STRING);
    }
private:
    bool is_mysql;
};


REGISTER_FUNCTION(Version)
{
    factory.registerFunction<FunctionVersion>(FunctionFactory::CaseInsensitive);
}

}
