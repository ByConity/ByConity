#include <Interpreters/prepared_statement.h>

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPreparedParameter.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_PREPARED_PARAMETER;
}

namespace
{
    struct ReplacePreparedParameter
    {
        using TypeToVisit = ASTPreparedParameter;

        const PreparedStatementContext & prepared_context;

        void visit(ASTPreparedParameter & prepared_param, ASTPtr & ast) const
        {
            const auto & param_name = prepared_param.name;
            ast = std::make_shared<ASTLiteral>(prepared_context.getParamValue(param_name));
        }
    };

    using ReplacePreparedParameterVisitor = InDepthNodeVisitor<OneTypeMatcher<ReplacePreparedParameter>, true>;
}

String toString(const PreparedParameterBindings & binding)
{
    String str;
    for (const auto & [param, value] : binding)
        str += param.toString() + " = " + applyVisitor(FieldVisitorToString(), value) + ", ";
    return str;
}

Field PreparedStatementContext::getParamValue(const String & param_name) const
{
    auto it = param_bindings.find(PreparedParameter{.name = param_name});
    if (it == param_bindings.end())
        throw Exception(ErrorCodes::BAD_PREPARED_PARAMETER, "Unresolved prepare parameter {}", param_name);
    return it->second;
}

void PreparedStatementContext::prepare(ASTPtr & ast) const
{
    if (!ast)
        return;
    ReplacePreparedParameter data{*this};
    ReplacePreparedParameterVisitor(data).visit(ast);
}

void PreparedStatementContext::prepare(ConstASTPtr & ast) const
{
    ASTPtr ptr = std::const_pointer_cast<IAST>(ast);
    prepare(ptr);
    ast = ptr;
}

void PreparedStatementContext::prepare(SizeOrVariable & size_or_variable) const
{
    if (!std::holds_alternative<String>(size_or_variable))
        return;

    const auto & param_name = std::get<String>(size_or_variable);
    // TODO: prevent negative number converted to UInt64
    UInt64 val = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), getParamValue(param_name));
    size_or_variable = size_t{val};
}
}
