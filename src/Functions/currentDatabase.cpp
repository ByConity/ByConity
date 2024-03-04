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
// currentDatabase(0)/currentDatabase() returns `dbname`. currentDatabase(1) returns `tenantid.dbname`
class FunctionCurrentDatabase : public IFunction
{
    const String db_name;

public:
    static constexpr auto name = "currentDatabase";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCurrentDatabase>(context->getCurrentDatabase());
    }

    explicit FunctionCurrentDatabase(const String & db_name_) : db_name{db_name_}
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
        String current_db_name = db_name;
        if (arguments.size() == 1)
        {
            const ColumnPtr col = arguments[0].column;
            if (col->size() && col->getUInt(0) != 0)
                current_db_name = getOriginalDatabaseName(db_name);
        }
        return DataTypeString().createColumnConst(input_rows_count, current_db_name);
    }
};

}

REGISTER_FUNCTION(CurrentDatabase)
{
    factory.registerFunction<FunctionCurrentDatabase>();
    factory.registerFunction<FunctionCurrentDatabase>("DATABASE", FunctionFactory::CaseInsensitive);
}

}
