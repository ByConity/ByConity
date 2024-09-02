#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>

#include <unordered_map>
#include <Poco/String.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** globalVariable('name') - takes constant string argument and returns the value of global variable with that name.
  * It is intended for compatibility with MySQL.
  *
  * Currently it's a stub, no variables are implemented. Feel free to add more variables.
  */
class FunctionGlobalVariable : public IFunction
{
public:
    static constexpr auto name = "globalVariable";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionGlobalVariable>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!checkColumnConst<ColumnString>(arguments[0].column.get()))
            throw Exception("Argument of function " + getName() + " must be constant string", ErrorCodes::BAD_ARGUMENTS);

        String variable_name = assert_cast<const ColumnConst &>(*arguments[0].column).getValue<String>();
        auto variable = global_variable_map.find(Poco::toLower(variable_name));
        if (variable == global_variable_map.end())
            return std::make_shared<DataTypeInt32>();
        else
            return variable->second.type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & col = arguments[0];
        String variable_name = assert_cast<const ColumnConst &>(*col.column).getValue<String>();
        auto variable = global_variable_map.find(Poco::toLower(variable_name));

        Field val = 0;
        if (variable != global_variable_map.end())
            val = variable->second.value;

        return result_type->createColumnConst(input_rows_count, val);
    }

private:
    struct TypeAndValue
    {
        DataTypePtr type;
        Field value;
    };
    std::unordered_map<String, TypeAndValue> global_variable_map
        = {{"max_allowed_packet", {std::make_shared<DataTypeInt32>(), 67108864}},
           {"version", {std::make_shared<DataTypeString>(), "5.7.0"}},
           {"version_comment", {std::make_shared<DataTypeString>(), ""}},
           {"auto_increment_increment", {std::make_shared<DataTypeInt32>(), 1}},
           {"character_set_client", {std::make_shared<DataTypeString>(), "utf8mb4"}},
           {"character_set_connection", {std::make_shared<DataTypeString>(), "utf8mb4"}},
           {"character_set_results", {std::make_shared<DataTypeString>(), "utf8mb4"}},
           {"character_set_server", {std::make_shared<DataTypeString>(), "utf8mb4"}},
           {"collation_server", {std::make_shared<DataTypeString>(), "utf8mb4_0900_ai_ci"}},
           {"collation_connection", {std::make_shared<DataTypeString>(), "utf8mb4_0900_ai_ci"}},
           {"init_connect", {std::make_shared<DataTypeString>(), ""}},
           {"interactive_timeout", {std::make_shared<DataTypeInt32>(), 28800}},
           {"license", {std::make_shared<DataTypeString>(), "GPL"}},
           {"lower_case_table_names", {std::make_shared<DataTypeInt32>(), 0}},
           {"net_write_timeout", {std::make_shared<DataTypeInt32>(), 60}},
           {"performance_schema", {std::make_shared<DataTypeInt32>(), 0}},
           {"sql_mode", {std::make_shared<DataTypeString>(), "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"}},
           {"system_time_zone", {std::make_shared<DataTypeString>(), "UTC"}},
           {"time_zone", {std::make_shared<DataTypeString>(), "UTC"}},
           {"wait_timeout", {std::make_shared<DataTypeInt32>(), 28800}},
           {"ssl_cipher", {std::make_shared<DataTypeString>(), "TLS_AES_256_GCM_SHA384"}},
           {"transaction_isolation", {std::make_shared<DataTypeString>(), "READ-UNCOMMITTED"}},
           {"tx_isolation", {std::make_shared<DataTypeString>(), "READ-UNCOMMITTED"}}};
};

}

REGISTER_FUNCTION(GlobalVariable)
{
    factory.registerFunction<FunctionGlobalVariable>();
}

}

