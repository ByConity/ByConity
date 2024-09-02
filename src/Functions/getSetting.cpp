#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/FieldToDataType.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Context.h>
#include <Core/Field.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Get the value of a setting.
class FunctionGetSetting : public IFunction, WithContext
{
public:
    static constexpr auto name = "getSetting";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGetSetting>(context_); }
    explicit FunctionGetSetting(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isString(arguments[0].type))
            throw Exception{"The argument of function " + String{name} + " should be a constant string with the name of a setting",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        const auto * column = arguments[0].column.get();
        if (!column || !checkAndGetColumnConstStringOrFixedString(column))
            throw Exception{"The argument of function " + String{name} + " should be a constant string with the name of a setting",
                            ErrorCodes::ILLEGAL_COLUMN};

        std::string_view setting_name{column->getDataAt(0)};
        /// Tenant is not allowed to get blacklist settings
        if (getContext()->shouldBlockPrivilegedOperations()
            && privilegedSettings.find(std::string(setting_name)) != privilegedSettings.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tenant cannot execute this function {} for security reason.", getName());

        value = getContext()->getSettingsRef().get(setting_name);

        DataTypePtr type = applyVisitor(FieldToDataType{}, value);
        value = convertFieldToType(value, *type);
        return type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, value);
    }

private:
    mutable Field value;
    static const std::unordered_set<std::string> privilegedSettings;
};

const std::unordered_set<std::string> FunctionGetSetting::privilegedSettings = { "s3_access_key_secret", "tos_secret_key", "lasfs_secret_key", "s3_ak_secret" };
}

REGISTER_FUNCTION(GetSetting)
{
    factory.registerFunction<FunctionGetSetting>();
}

}
