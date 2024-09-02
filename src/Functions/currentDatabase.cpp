#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

// Get current database name. Parameter decides if the return db name contains tenantID.
// currentDatabase(0) returns `tenantid.dbname`. others returns `dbname`
class FunctionCurrentDatabase : public IFunction
{
    const String db_name;
    const String db_name_without_tenant_id;

public:
    static constexpr auto name = "currentDatabase";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCurrentDatabase>(context->getCurrentDatabase());
    }

    explicit FunctionCurrentDatabase(const String & db_name_) :
        db_name{db_name_}, db_name_without_tenant_id{getOriginalDatabaseName(db_name_)}
    {
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 1 || (arguments.size() == 1 && !isNumber(arguments[0])))
            throw Exception("Function " + getName() + " can only accept one integer argument at most.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, has_argument_0(arguments) ? db_name : db_name_without_tenant_id);
    }

private:
    static bool has_argument_0(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 1)
            return false;
        const ColumnPtr & col = arguments[0].column;
        return col->size() && col->getUInt(0) == 0;
    }
};

}

REGISTER_FUNCTION(CurrentDatabase)
{
    factory.registerFunction<FunctionCurrentDatabase>();
    factory.registerFunction<FunctionCurrentDatabase>("DATABASE", FunctionFactory::CaseInsensitive);
    factory.registerAlias("schema", FunctionCurrentDatabase::name, FunctionFactory::CaseInsensitive);
}

}
