#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <common/variant_helper.h>

#include <fmt/format.h>

#include <unordered_map>
#include <unordered_set>
#include <variant>

namespace DB
{

// types for data which can be a prepared param
using SizeOrVariable = std::variant<size_t, String>;
using UInt64OrVariable = std::variant<UInt64, String>;

struct PreparedParameter
{
    String name;
    DataTypePtr type;

    struct Hash
    {
        std::size_t operator()(const PreparedParameter & e) const
        {
            return std::hash<String>()(e.name);
        }
    };

    bool operator==(const PreparedParameter & o) const
    {
        return name == o.name;
    }

    String toString() const
    {
        return fmt::format("[{}:{}]", name, type->getName());
    }
};

using PreparedParameterSet = std::unordered_set<PreparedParameter, PreparedParameter::Hash>;
using PreparedParameterBindings = std::unordered_map<PreparedParameter, Field, PreparedParameter::Hash>;
String toString(const PreparedParameterBindings & binding);

class PreparedStatementContext : public WithContext
{
public:
    explicit PreparedStatementContext(PreparedParameterBindings param_bindings_, const ContextPtr & context_)
        : WithContext(context_), param_bindings(std::move(param_bindings_))
    {
    }

    Field getParamValue(const String & param_name) const;
    void prepare(ASTPtr & ast) const;
    void prepare(ConstASTPtr & ast) const;
    void prepare(SizeOrVariable & size_or_variable) const;

private:
    PreparedParameterBindings param_bindings;
};
}
