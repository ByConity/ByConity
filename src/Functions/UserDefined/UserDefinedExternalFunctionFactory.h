#pragma once

#include <functional>
#include <memory>
#include <set>
#include <shared_mutex>
#include <string>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Field.h>
#include <Functions/IFunction.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExternalFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Functions/UserDefined/UDFFlags.h>

namespace DB
{

class UserDefinedExternalFunctionFactory
{
public:
    static UserDefinedExternalFunctionFactory & instance();

    UserDefinedExternalFunctionFactory();

    FunctionOverloadResolverPtr tryGet(const String & function_name);

    AggregateFunctionPtr tryGet(const String & function_name, const DataTypes & argument_types, const Array & parameters);

    bool has(const String & function_name, UDFFunctionType type = UDFFunctionType::Count);

    bool tryGetVersion(const String & function_name, uint64_t **version_ptr);

private:
    friend class UserDefinedSQLObjectsLoader;
    friend class UserDefinedSQLFunctionFactory;

    bool setFunction(const String & name, const IAST &ast, const ContextPtr &context);

    bool removeFunction(const String & function_name);

    void setAllFunctions(std::vector<std::pair<String, ASTPtr>> & asts, const ContextPtr &context);

    using WriteLock = std::unique_lock<std::shared_mutex>;
    using ReadLock = std::shared_lock<std::shared_mutex>;

    std::unordered_map<String, ASTExternalFunction::Arguments> functions[to_underlying(UDFFunctionType::Count)];
    std::shared_mutex mutex;
    Poco::Logger * log;
};

}
